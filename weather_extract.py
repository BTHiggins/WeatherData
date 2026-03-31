# %pip install openmeteo-requests
# %pip install requests-cache retry-requests
# %pip install polars
# %pip install duckdb
# %pip install Numpy


import openmeteo_requests
import polars as pl
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta, timezone
import duckdb
import pyarrow
from prefect.blocks.system import Secret

# api config
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

url = "https://api.open-meteo.com/v1/forecast"


# Api call configuration ----------------------------------------------------
# Oxford
location = "Oxford"
latitude = 52.52
longitude = 13.41
forecast_model = "ukmo_seamless"

var_list = [
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
    "cloud_cover_2m"
]

params = {
	"latitude": latitude,
	"longitude": longitude,
	"hourly": var_list,
    "models": forecast_model
}

# api call --------------------
responses = openmeteo.weather_api(url, params=params)

# loop through responses if multiple locatins
response = responses[0]
hourly = response.Hourly()

# Dataframe processing ----------------------
# Setting up the datetime length to create the dataframe with
start = datetime.fromtimestamp(hourly.Time(), timezone.utc)
end = datetime.fromtimestamp(hourly.TimeEnd(), timezone.utc)
freq = timedelta(seconds = hourly.Interval())

# Dataframe base
df_base = pl.select(
    forecast_model = pl.lit(forecast_model),
    location = pl.lit(location),
    lat = response.Latitude(),
    lon = response.Longitude(),
    extract_date = datetime.fromtimestamp(hourly.Time(), timezone.utc).date(),
    extract_time = datetime.now().time(),
    fc_datetime = pl.datetime_range(start, end, freq, closed="left")
).with_columns(
    fc_date = pl.col('fc_datetime').dt.date(),
    fc_hour = pl.col('fc_datetime').dt.strftime("%H"),
    row_id = pl.concat_str(
        [
            pl.col('location'),
            pl.col('lat'),
            pl.col('lon'),
            pl.col('extract_date'),
            pl.col('extract_time'),
            pl.col('fc_datetime')
        ]   
    )
)

# dataframe metrics - dictionary expression so variables can be easily changed above - this will mess up the duckdb tables though
col_exprs = {var: hourly.Variables(var_list.index(var)).ValuesAsNumpy() for var in var_list}

df_metrics = pl.DataFrame(
    col_exprs
)

# output dataframe - converting to arrow to use in motherduck
df_raw = pl.concat([df_base, df_metrics], how='horizontal')
# df_raw = df_raw.to_arrow()


# Initialise the duckdb connection -------------------------------------
db_token = Secret.load("duckdb-token")
db_name = "WeatherData"

con = duckdb.connect(f'md:?"motherduck_token={db_token}')

# Update staging and main forecast tables ----------------------------------
# overwrite staging table
overwrite_staging_table = f"""
    CREATE OR REPLACE TABLE {db_name}.forecast_staging AS 
    SELECT 
        * 
    FROM 
        df_raw;
"""

con.sql(overwrite_staging_table)

# On first run, main might not exist, should be always skipped
create_forecast_table = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.forecast AS
    SELECT
        *
    FROM 
        df_raw;
"""

con.sql(create_forecast_table)


# Update main table
append_staging_to_main = f"""
    INSERT INTO 
        {db_name}.forecast
    SELECT 
        * 
    FROM 
        {db_name}.forecast_staging
    ON CONFLICT DO NOTHING;
"""

con.sql(append_staging_to_main)


# verify the main table has the latest update date (today)
verify_update = f"""
    SELECT
        MAX() AS _max_date
    FROM
        {db_name}.forecast
"""

_max_date_db = con.sql(verify_update)
_max_date_local = df_raw.select(pl.max('fc_datetime'))
assert _max_date_db == _max_date_local, "Latest datetimes do not match, need investigation."

