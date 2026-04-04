# WeatherData
Prefect + Motherduck + Claude: Setting up a simple and free AI analytics pipeline 
DuckDB has become more and more useful to me as I handle big datasets both locally and in cloud environments. Looking for more ways to 

- user of duck db, motherduck looked interesting, dives looked too easy, can I try?


- Aim - setup a free pipeline fully in the cloud
    - Prefect, free option that is fully managed, no setting up a docker container and dealing with extra auth
    - Motherduck, big fan of duckdb and wanted to get more handson with their cloud offering. This also has a free tier.
        - On investigating, dives leveraging ai looks interesting. Free AI for analysis is great
    - Claud, this and chatgpt, seems the easiest to set up, opinion claud is the lesser of the two evils (interpret as you will)
- Wanted a use case that means I wouldn't need to handle anything on my local machine - fully in the cloud.
- Planned to use motherduck database as storage, no blob or lakehouse, keep it simple
- Open-meteo has a great api, also has a generated python script to get going
- Tested the api and investigated the data using polars as my dataframe thing
- Data looks good, how can i get started in motherduck
- Different options, go for pulse.
- Documentation fairly straight forward, set up and account and generated an auth token. No significant auth setup, all based on the permissions given to the key.
- Created a weather data database with the intention for the pipeline to handle the table creation and updating.
- Little test in python to check that I could connect, successful
- Prefect, built by airflow peerson, very similar and I have dabbled in that. 
- Originally had a time in nanoseconds, duckdb can only go down to microseconds
