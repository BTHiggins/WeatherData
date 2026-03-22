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