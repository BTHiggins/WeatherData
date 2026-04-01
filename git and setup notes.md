- Setup the git repo on github first then go the local drive and tie it to the online repo

In bash do: pip freeze > requirement.txt
echo requirements.txt to read back in bash

python -m venv .venv
then activate it in the Scripts folder of the venv

remember to create a gitignore and add ".venv" to it before committing



.venv\Scripts\activate

# to authenticate the device for duckdb
python -c "import duckdb; con = duckdb.connect('md:'); print('authenticated')"



## Prefect
1. Create a prefect cloud account
2. In a venv
    ```
    pip install prefect
    pip install prefect-cloud
    ```
3. 
    ```
    prefect-cloud login
    ```
4. Run a test prefect script
    ```
    python prefec_test_1.py
    ```
5. The python file needs to be uploaded to github for it to be deployed. The prefect cloud github app also needs to be installed for authentication on prvate githubs.
6. Create a deployment (repeatable flow workspace)
    ```
    prefect-cloud deploy prefect_test_1.py:main \
    --name bh_test_deployment
    ```
7. Once deployed can now schedule:
    ```
    # 3pm everyday  
    prefect-cloud schedule main/bh_test_deployment "0 15 * * *"
    ```
    Can also schedule in prefect cloud in the deployment section
    
8. Save secrets like this:
    ```
    from prefect.blocks.system import Secret

    secret = Secret(value="Marvin's surprise birthday party is on the 15th")
    secret.save("surprise-party")
    ```
    Recall them like this:
    ```
    from prefect import flow    
    from prefect.blocks.system import Secret

    @flow
    def my_flow():
        secret = Secret.load("surprise-party")
        print(secret.get())
    ```
    Just run the python file in the terminal (python file_name.py) and as long as the terminal is already logged into prefect cloud it will work.



1. Deploy from github:
    uvx prefect-cloud deploy <path/to/script.py:entrypoint_function_name> \
    --from <github-account>/<repo-name> \
    --name <deployment_name>

    uvx prefect-cloud deploy weather_extract.py:weather_forecast_pipeline \
    --from BTHiggins/WeatherData \
    --name weather-forecast-deployment
# Note: The --name needs to be the same as specified in the yaml config file

# To update the deployment, run "prefect deploy" in the terminal



DUCKDB auth errors:
1. Create a service account
2. Store the token in prefect secrets block
3. Create a share link from the database in motherduck, "anyone in the org".
    - The service account being a member means they can access the motherduck instance but does not grant them rights to every database.
# NOTE: 
    - The service account token did not work, instead I simply created an access token for the pipeline.