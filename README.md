# Mobility in Brussels

## Introduction
The goal of this project is to get Brussels mobility data that are available to the public.

Ultimately, we want to display traffic data as well as other mobility data on a dynamic map of Brussels. 
You can check the latest version of our map on our [Tableau Online link](https://eu-west-1a.online.tableau.com/t/thimremyanalytics/views/BrusselsMobility-BigQuery/TrafficinBrussels1-mininterval?:device=phone&:showAppBanner=false&:devicepreviewmode=true&:display_count=n&:showVizHome=n&:origin=viz_share_link) (you will have to create an account).

For the moment, we only have snapshots of real-time data that were extracted at certain points in time.

## Brussels open datastore

### Traffic counts api

On several locations in the Brussels Region, traffic is measured using magnetic loops or cameras. To access this data, we can use the API or the geowebservices. For the moment, only real-time data is available, historical data is coming.

In this project, we use the API. For more information on the latter, please visit the [Brussels open datastore documentation](https://data-mobility.brussels/traffic/api/counts/).

In the `Traffic counts api` folder, you'll find 

- the `API to CSV` folder explaining how to extract data via the public API and store them in .csv files. The folder contains:
    - a Jupyter Notebook detailing how to get the latest livestream data (the last 1-min interval available) for all detectors by   traverse or by lane each time the code is run. We're going to extract, among other things, the number of vehicules passed between start and end time as well as the average speed of those counted vehicules.
    - 2 .csv files (outputs of the code once run):
      - `traverse_devices.csv`: the details of each device (magnetic loop or camera) such as name, longitude, latitude, etc.
      - `traverse_live.csv`: the traffic data measured by all devices at several points in time such as the number of vehicules passed during the 1-min interval, the average speed of those counted vehicules, etc.
- the `API to BQ` folder explaining how to extract data via the public API and store them in BigQuery tables. The folder contains:
    - a Jupyter Notebook detailing the same procedure as the one in the `API to CSV` folder except that the outputs are stored in BigQuery tables. The notebook was developed in Google Cloud Datalab in the Google Cloud Platform environment in order to easily create BigQuery tables.
    - the `Google Cloud Function` subfolder containing: 
      - the `main.py` script that we scheduled to be run every minute in order to update our BigQuery tables. You'll notice that we transformed the code you can find in the Jupyter Notebook into functions so that it can be triggered by a scheduler. __We detail how we did implement scheduled refresh of our database in a below section__.
      - the `requirements.txt` which detail the specific version of some libraries that are needed.
      
### Bike counts api

The number of bikers are counted on several locations in the Brussels Region. The number of counters will be extended in the next years. To access the data, we can use the API or the geowebservices. Real time and historical data are available.

In this project, we use the API. For more information on the latter, please visit the [Brussels open datastore documentation](https://data-mobility.brussels/bike/api/counts/).

In the `Bike counts api` folder, you'll find a Jupyter Notebook detailing how to get the latest livestream data for all bike counting poles each time the code is run. We're going to extract the livestream count with the number of bikers passed last hour, day and year. The counts are only updated when a bike passes the sensor.

You'll also find in the folder 2 .csv files (outputs of the code once run):
- `pole_devices.csv`: the details of each bike counting pole such as name, longitude, latitude, etc.
- `pole_live.csv`: the information captured by all bike counting poles at several points in time such as the number of bikers passed last hour, etc.

## How our BigQuery Tables are updated in Google Cloud Platform environment and then published in Tableau

### Use of Google Scheduler and Google Cloud Function

1. In order to update our BigQuery tables every minute, we used defined a Google Cloud Function (cfr. `main.py` script) that will add livestream traffic data to our BigQuery tables each time  it is run.
2. We then used Google Cloud Scheduler to schedule the function to be run every minute.

### Link with Tableau

3. We connected directly our Tablea dashboard to our BigQuery tables through the user interface.
4. Finally, we set up a hourly refresh of our dataset through Tableau.  

## Next steps

- Improve current visualizations and develop new ones in Tableau
- Shift from .csv files to BigQuery tables to store bike information
- Add bike information on the map
- Add Villo! real-time data on the map
- Develop AI models to predict traffic and usage of bikes
