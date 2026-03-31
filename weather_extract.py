# %pip install openmeteo-requests requests-cache retry-requests polars duckdb pyarrow prefect

import openmeteo_requests
import polars as pl
import requests_cache
import duckdb
from retry_requests import retry
from datetime import datetime, timedelta, timezone

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret


# ---------------------------------------------------------------------------
# Config — keep outside tasks so it's easy to override / parametrise the flow
# ---------------------------------------------------------------------------

LOCATION = "Oxford"
LATITUDE = 51.75   
LONGITUDE = -1.25
FORECAST_MODEL = "ukmo_seamless"
API_URL = "https://api.open-meteo.com/v1/forecast"
DB_NAME = "WeatherData"

VAR_LIST = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "showers",
    "snowfall",
    "hail",
    "weather_code",
    "visibility",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover",
    "cloud_cover_2m",
]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch-weather-data", retries=3, retry_delay_seconds=10)
def fetch_weather_data(
    latitude: float,
    longitude: float,
    var_list: list[str],
    forecast_model: str,
) -> object:
    """Call the Open-Meteo API and return the raw response object."""
    logger = get_run_logger()
    logger.info(f"Fetching weather data — lat={latitude}, lon={longitude}, model={forecast_model}")

    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": var_list,
        "models": forecast_model,
    }

    responses = client.weather_api(API_URL, params=params)
    logger.info("API call successful")

    # single-location response
    return responses[0]   


@task(name="build-dataframe")
def build_dataframe(
    response,
    location: str,
    forecast_model: str,
    var_list: list[str],
) -> pl.DataFrame:
    """Transform the raw Open-Meteo response into a Polars DataFrame."""
    logger = get_run_logger()
    hourly = response.Hourly()

    start = datetime.fromtimestamp(hourly.Time(), timezone.utc)
    end   = datetime.fromtimestamp(hourly.TimeEnd(), timezone.utc)
    freq  = timedelta(seconds=hourly.Interval())

    df_base = pl.select(
        forecast_model=pl.lit(forecast_model),
        location=pl.lit(location),
        lat=response.Latitude(),
        lon=response.Longitude(),
        extract_date=datetime.fromtimestamp(hourly.Time(), timezone.utc).date(),
        extract_time=datetime.now().time(),
        fc_datetime=pl.datetime_range(start, end, freq, closed="left"),
    ).with_columns(
        fc_date=pl.col("fc_datetime").dt.date(),
        fc_hour=pl.col("fc_datetime").dt.strftime("%H"),
        row_id=pl.concat_str(
            [
                pl.col("location"),
                pl.col("lat").cast(pl.Utf8),
                pl.col("lon").cast(pl.Utf8),
                pl.col("extract_date").cast(pl.Utf8),
                pl.col("extract_time").cast(pl.Utf8),
                pl.col("fc_datetime").cast(pl.Utf8),
            ]
        ),
    )

    col_exprs = {
        var: hourly.Variables(var_list.index(var)).ValuesAsNumpy()
        for var in var_list
    }
    df_metrics = pl.DataFrame(col_exprs)

    df_raw = pl.concat([df_base, df_metrics], how="horizontal")
    logger.info(f"DataFrame built — {df_raw.shape[0]} rows, {df_raw.shape[1]} columns")
    return df_raw


@task(name="get-db-connection")
def get_db_connection(db_name: str) -> duckdb.DuckDBPyConnection:
    """Load the MotherDuck token from a Prefect Secret and open a connection."""
    logger = get_run_logger()
    db_token = Secret.load("duckdb-token").get()
    con = duckdb.connect(f"md:{db_name}?motherduck_token={db_token}")
    logger.info(f"Connected to MotherDuck database: {db_name}")
    return con


@task(name="write-to-duckdb")
def write_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    df_raw: pl.DataFrame,
    db_name: str,
) -> duckdb.DuckDBPyConnection:
    """
    1. Overwrite the staging table.
    2. Create the main forecast table if it doesn't exist.
    3. Upsert from staging → main (ON CONFLICT DO NOTHING via row_id PK).
    """
    logger = get_run_logger()

    con.sql(f"""
        CREATE OR REPLACE TABLE {db_name}.forecast_staging AS
        SELECT * FROM df_raw
    """)
    logger.info("Staging table overwritten")

    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.forecast AS
        SELECT * FROM df_raw WHERE FALSE
    """)

    con.sql(f"""
        INSERT INTO {db_name}.forecast
        SELECT * FROM {db_name}.forecast_staging
        ON CONFLICT DO NOTHING
    """)
    logger.info("Upsert into main forecast table complete")



@task(name="verify-update")
def verify_update(
    con: duckdb.DuckDBPyConnection,
    df_raw: pl.DataFrame,
    db_name: str,
) -> None:
    """Assert that the max fc_datetime in the DB matches the local DataFrame."""
    logger = get_run_logger()

    result = con.sql(f"""
        SELECT MAX(fc_datetime) AS _max_date
        FROM {db_name}.forecast
    """).fetchone()

    _max_date_db = result[0]
    _max_date_local = df_raw.select(pl.max("fc_datetime")).item()

    logger.info(f"DB max fc_datetime: {_max_date_db}")
    logger.info(f"Local max fc_datetime: {_max_date_local}")

    assert _max_date_db == _max_date_local, (
        f"Latest datetimes do not match — DB: {_max_date_db}, local: {_max_date_local}. "
        "Needs investigation."
    )
    logger.info("Verification passed ✓")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="weather-forecast-pipeline", log_prints=True)
def weather_forecast_pipeline(
    location: str = LOCATION,
    latitude: float = LATITUDE,
    longitude: float = LONGITUDE,
    forecast_model: str = FORECAST_MODEL,
    var_list: list[str] = VAR_LIST,
    db_name: str = DB_NAME,
) -> None:
    """End-to-end pipeline: fetch -> transform -> load -> verify."""

    response = fetch_weather_data(latitude, longitude, var_list, forecast_model)
    df_raw = build_dataframe(response, location, forecast_model, var_list)
    con = get_db_connection(db_name)

    write_result = write_to_duckdb(con, df_raw, db_name)
    verify_update(con, df_raw, db_name, wait_for=[write_result])


if __name__ == "__main__":
    weather_forecast_pipeline()