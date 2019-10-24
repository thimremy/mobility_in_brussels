import requests
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
import pandas_gbq

# General remark: the functions detailed in this script can be improved to limit redundancies. It will be undertaken in a next step.

# Below function will get device data (name, location, etc. of the traverses and their detectors by lane) and create an json object to store it.
def get_device_data():
    traverse_devices_response = requests.get("http://data-mobility.brussels/traffic/api/counts/?request=devices")
    traverse_devices_status_code = traverse_devices_response.status_code
    traverse_devices_content = traverse_devices_response.content
    decoded_traverse_devices_content = traverse_devices_content.decode('utf-8') # Decode using the utf-8 encoding
    json_traverse_devices_content = json.loads(decoded_traverse_devices_content)
    
    return json_traverse_devices_content

# Below function extracts the real-time data off all the traverse and create an json object to store it. 
def get_live_data():
    parameters = {'request': 'live', 'interval': '1', 'singleValue': 'true'}
    traverse_live_response = requests.get("http://data-mobility.brussels/traffic/api/counts/", params=parameters)
    traverse_live_status_code = traverse_live_response.status_code
    traverse_live_content = traverse_live_response.content
    decoded_traverse_live_content = traverse_live_content.decode('utf-8') # Decode using the utf-8 encoding
    json_traverse_live_content = json.loads(decoded_traverse_live_content)
    
    return json_traverse_live_content

# Below function creates the list of traverse names as we'll need it to extract live data.
def get_traverse_name():
    json_traverse_devices_content = get_device_data()
    
    list_of_traverse_name = []
    for item in json_traverse_devices_content["features"]:
        traverse_name = item["properties"]["traverse_name"]
        list_of_traverse_name.append(traverse_name)
    
    return list_of_traverse_name

# Function to check if a BigQuery table already exists or not.
def doesTableExist(project_id, dataset_id, table_id):
    bigquery_client = bigquery.Client(project_id)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        table = bigquery_client.get_table(table_ref)
        if table:
            print("Table {}\'s existence sucessfully proved!".format(table_ref))
            return True
    except NotFound as error:
        # ...do some processing ...
        print("Whoops! Table {} doesn\'t exist here! Ref: {}".format(table_ref, error))
        return False

# Below function extract the device data from a json object and store them in a DataFrame.
def device_json_to_df():
    json_traverse_devices_content = get_device_data()
    
    traverse_devices_df = pd.DataFrame(columns = ["traverse_request_date", "traverse_id", "traverse_name", "traverse_descr_nl", 
                                              "traverse_descr_fr", "traverse_descr_en", "traverse_longitude", 
                                              "traverse_latitude", "traverse_orientation", "traverse_number_of_lanes", 
                                              "detector_1", "detector_2", "detector_3", "detector_4", "detector_5"])
    
    traverse_request_date = json_traverse_devices_content["requestDate"]
    i = 0
    for item in json_traverse_devices_content['features']:
        traverse_id = item["id"]
        traverse_longitude = item["geometry"]["coordinates"][0]
        traverse_latitude = item["geometry"]["coordinates"][1]
        traverse_name = item["properties"]["traverse_name"]
        traverse_descr_nl = item["properties"]["descr_nl"]
        traverse_descr_fr = item["properties"]["descr_fr"]
        traverse_descr_en = item["properties"]["descr_en"]
        traverse_orientation = item["properties"]["orientation"]
        traverse_number_of_lanes = item["properties"]["number_of_lanes"]

        detector_dict = dict.fromkeys(["detector_1", "detector_2", "detector_3", "detector_4", "detector_5"])
        detector_list = ["detector_1", "detector_2", "detector_3", "detector_4", "detector_5"]
        det_count = 0
        for detector in item["properties"]["detectors"]:
            detector_dict[detector_list[det_count]] = detector
            det_count += 1
        traverse_devices_df.loc[i] = [traverse_request_date, traverse_id, traverse_name, traverse_descr_nl, traverse_descr_fr, 
                                      traverse_descr_en, traverse_longitude, traverse_latitude, traverse_orientation, 
                                      traverse_number_of_lanes, detector_dict["detector_1"], detector_dict["detector_2"], 
                                      detector_dict["detector_3"], detector_dict["detector_4"], detector_dict["detector_5"]]  
        i += 1
        
    coordinates = ["traverse_longitude", "traverse_latitude"]
    for coord in coordinates:
        traverse_devices_df[coord] = traverse_devices_df[coord].astype(str).str.replace(".", ",")
  
    int_cols = ["traverse_orientation", "traverse_number_of_lanes"]
    for col in int_cols:
        traverse_devices_df[col] = traverse_devices_df[col].astype(int)
        
    traverse_devices_df.drop("traverse_descr_en", axis=1, inplace=True)
    traverse_devices_df.loc[9,"traverse_descr_fr"] = traverse_devices_df.loc[9,"traverse_descr_nl"]
    
    return traverse_devices_df

# Below function extract the latest livestream data from a json object and store them in a DataFrame.
def live_json_to_df():
    json_traverse_live_content = get_live_data()
    list_of_traverse_name = get_traverse_name()
    
    traverse_live_df = pd.DataFrame(columns = ['traverse_live_request_date', 'traverse_name', 'traverse_interval', 'traverse_count', 
                                           'traverse_speed', 'traverse_occupancy','traverse_start_time', 'traverse_end_time'])
    
    traverse_live_request_date = json_traverse_live_content["requestDate"]
    traverse_interval = '1m'
    i = 0
    for col in list_of_traverse_name:
        traverse_name = col
        traverse_count = json_traverse_live_content["data"][col]["results"][traverse_interval]["count"]
        traverse_speed = json_traverse_live_content["data"][col]["results"][traverse_interval]["speed"]
        traverse_occupancy = json_traverse_live_content["data"][col]["results"][traverse_interval]["occupancy"]
        traverse_start_time = json_traverse_live_content["data"][col]["results"][traverse_interval]["start_time"]
        traverse_end_time = json_traverse_live_content["data"][col]["results"][traverse_interval]["end_time"]

        traverse_live_df.loc[i] = [traverse_live_request_date, traverse_name, traverse_interval, traverse_count, traverse_speed, 
                                   traverse_occupancy, traverse_start_time, traverse_end_time]
        i += 1

    traverse_live_df.dropna(inplace = True)
    
    traverse_live_df["traverse_count"] = traverse_live_df["traverse_count"].astype(int)
    
    traverse_live_df["traverse_live_request_date"] = pd.to_datetime(traverse_live_df["traverse_live_request_date"], format='%Y/%m/%d %H:%M')
    traverse_live_df["traverse_start_time"] = pd.to_datetime(traverse_live_df["traverse_start_time"], format='%Y/%m/%d %H:%M', errors = 'coerce')
    traverse_live_df["traverse_end_time"] = pd.to_datetime(traverse_live_df["traverse_end_time"], format='%Y/%m/%d %H:%M', errors = 'coerce')
    traverse_live_df["traverse_end_date"] = traverse_live_df["traverse_end_time"].dt.date
    traverse_live_df["traverse_end_date"] = pd.to_datetime(traverse_live_df["traverse_end_date"], format='%Y/%m/%d')
    traverse_live_df["traverse_end_hour"] = traverse_live_df["traverse_end_time"].dt.time

    return traverse_live_df

# Below function overwrites the existing device table if detectors are added.
def device_df_to_gbq():
    traverse_devices_df = device_json_to_df()
    
    # TO DO: uncomment the below line and define where to store your table.
	
	# project_id = ""
	# dataset_id = ""
	# table_id = ""
	dataset_table_id = dataset_id + "." + table_id

	if not doesTableExist(project_id,dataset_id,table_id):
		try:
			pandas_gbq.to_gbq(traverse_devices_df, dataset_table_id, project_id=project_id, if_exists='fail')
		except:
			pass
	else:
		old_traverse_devices_df = pandas_gbq.read_gbq("SELECT traverse_name from {}".format(dataset_table_id), project_id = project_id)
		if traverse_devices_df.shape[0] != old_traverse_devices_df.shape[0]:
			pandas_gbq.to_gbq(traverse_devices_df, dataset_table_id, project_id=project_id, if_exists='replace')

# Below function appends new rows to the table containing livestream data of each device.
def live_df_to_gbq():
    traverse_live_df = live_json_to_df()
    
	# TO DO: uncomment the below line and define where to store your table.

	# project_id = ""
	# dataset_id = ""
	# table_id = ""
	dataset_table_id = dataset_id + "." + table_id

	if not doesTableExist(project_id,dataset_id,table_id):
		try:
			pandas_gbq.to_gbq(traverse_live_df, dataset_table_id, project_id=project_id, if_exists='fail')
		except:
			pass
	else:
		old_traverse_live_df = pandas_gbq.read_gbq("SELECT * from {}".format(dataset_table_id), project_id=project_id)

		for index, row in traverse_live_df.iterrows():
			traverse_live_last_update = row["traverse_end_time"]
			traverse_name = row["traverse_name"]
			old_traverse_live_last_update = old_traverse_live_df.loc[old_traverse_live_df["traverse_name"] == traverse_name, 
																	 "traverse_end_time"].max().tz_localize(None)
			if traverse_live_last_update <= old_traverse_live_last_update:
				traverse_live_df.drop(index, axis=0, inplace=True)

		pandas_gbq.to_gbq(traverse_live_df, dataset_table_id, project_id=project_id, if_exists='append')

# This function when triggered starts the whole process of updating Brussels traffic data.
def final(request):
    device_df_to_gbq()
    live_df_to_gbq()