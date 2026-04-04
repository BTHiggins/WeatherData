# Prefect + Motherduck + Claude: Setting up a simple and free AI analytics pipeline 

## Summary
Putting LLMs on top of data is powerful and becoming really easy to do, the pipeline I set up using Prefect, MotherdDck, and Claude proves that. At a small scale, staying within consumption limists, it can be set up with no cost - great for hobbyists and ephemeral analyses/visualisations. My example below gets a small amount of data from a weather forecast API, persists the data in Motherduck, then uses Claude AI to query the data and generate visuals. 

## Technology used
My goal was to setup a low/no cost pipeline fully in cloud infrastructure - MotherDuck and Prefect having free cloud options fit the bill (or lack thereof).

#### MotherDuck
- Free cloud option
- Simple authentication
- Already a user of duckdb 

### Prefect
- Free cloud option
- Similar to Airflow and Spark pipelines
- Deployments straight from GitHub

### Claude
- Simple integration into MotherDuck dives (I chose MotherDuck first, the chicken, or duck in this case, came before the Claude-shaped egg).

## Getting things up and running

### Data source
I was looking to set something up with a small amount of data. Weather data is great, as a small amount of data points is able to create something meaningful. There are a few options for weather forecast APIs, I went with [open-meteo](https://open-meteo.com/). It has a really useful response builder and a good amount of weather measurements.

I would be pulling forecast data over the next 7 days and transforming it using the python polars module. The prefect script contains the API pull and subsequent transformations.

![open-meteo](https://raw.githubusercontent.com/BTHiggins/WeatherData/main/Images/1%20-%20OpenMeteo.png)

### Setting up MotherDuck
MotherDuck setup is straight forward. You create an account and choose the instance you need, I used "pulse" as this is free within the consumption limits. 

To connect to MotherDuck in scripts we need to set up authentication tokens which automatically link to the instance the token was generated from and apply the associated permissions configured when creating the token. 

A quick test shows the token is working as expected:

![token-check](https://raw.githubusercontent.com/BTHiggins/WeatherData/main/Images/2%20-%20Duckdb%20auth%20test.png)

I created a database called "WeatherData" and did a quick test via a python script to check I could read and write from it.

### Setting up Prefect
Like MotherDuck, setting up Prefect cloud is as easy as creating an account and choosing the free cloud offering. Make sure to link prefect to GitHub; it reads straight from the repository, nice and easy. 

You can set up GitHub actions to keep the deployment in sync with the repo, but as I'm doing this pretty quickly I used the prefect cloud cli tool to update the deployment from GitHub on demand.

After reading the documentation ~~and spending 2 hours troublshooting why it wouldn't write to MotherDuck~~, I set up all my tasks and dependencies in the python dag:
API pull --> Transform with polars --> write to MotherDuck. The deployment definition and requirements are defined in the 'prefect.yaml' file and the MotherDuck connection token is stored in a Prefect secrets block.


### Integrating claude
MotherDuck has a feature called "Dives" which is where you can save your AI generated visuals and queries. Any LLM can be used with MotherDuck for dives, I went with Claude as it has a pre-built connector for MotherDuck. MotherDuck has easy to follow documentation for setting this up.


## In Action
The Prefect deployment is scheduled to run daily at 7am. It's been successful over the last few days so MotherDuck should have some usable data.
![Prefect-pipeline](https://raw.githubusercontent.com/BTHiggins/WeatherData/main/Images/4%20-%20Prefect%20pipeline.png)

In MotherDuck, we can query our tables in a notebook and get a view of the data we're working with. I created two tables, one main table that contains all the API extracts with their extract datetimes and a secondary staging table that is overwritten with each extract.
![MD data landing](https://raw.githubusercontent.com/BTHiggins/WeatherData/main/Images/3%20-%20MD%20data%20landing.png)

It's time to start interfacing with Claude to spin up a dive. A good starting point is a simple prompt to ensure it's connected all ok - "List all my databases on MotherDuck".
![Claude 1](https://raw.githubusercontent.com/BTHiggins/WeatherData/main/Images/5%20-%20Claude%20AI%20MD%20link.png)


I asked Claude to explore the database and it gave a summary:
![Claude 2](https://raw.githubusercontent.com/BTHiggins/WeatherData/main/Images/6%20-%20Calude%20AI%20MD%20explore.png)

I then asked Claude to create a dive. The prompt I used was "Create a Dive based on the forecast temperature for the next 6 hours and the following 6 days. Make sure the data is prefiltered to the latest extract date". Claude queried the data in MotherDuck and generated a dashboard-type visual page using React - very cool. 
![Claude 3](https://raw.githubusercontent.com/BTHiggins/WeatherData/main/Images/7%20-%20ClaudAI%20Dive%20Finished.png)

You can then get Claude to tweak it as you like and fix any errors, the end result is a report that is linked live to the MotherDuck data. Once happy, you need to ask Claude to save it. This dive then persists in MotherDuck and can be used without having to interface with Claude again.
![Claude 4](https://raw.githubusercontent.com/BTHiggins/WeatherData/main/Images/8%20-%20MD%20Dive.png)

You can 'remix' (edit) a dive from MotherDuck, this loads up the Claude web page with an auto prompt so Claude knows which dive is in scope. I found my Claude tokens/usage quickly hit it's daily limit on the free version - this might be fairly token heavy but I have no stats on this for the time being. 
![Claude 5](https://raw.githubusercontent.com/BTHiggins/WeatherData/main/Images/9%20-%20MD%20dive%20remix.png)

That's it! This workflow is suprisingly simple for setting up something so powerful. This pipeline can be scaled up by using the paid offerings from MotherDuck and Prefect, but fundamentally the workflow does not change. 

