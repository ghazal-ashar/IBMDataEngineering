import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file):
    dataframe = pd.read_csv(file)
    return dataframe

def extract_from_json(file):
    dataframe = pd.read_json(file, lines = True)
    return dataframe

def extract_from_xml(file):
    dataframe = pd.DataFrame(columns = ["name", "height", "weight"])
    tree = ET.parse(file)
    root = tree.getroot()

    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)

        dataframe = pd.concat([dataframe, pd.DataFrame([{"name" : name, "height" : height, "weight" : weight}])], ignore_index = True)
    return dataframe


def extract():

    extracted_data = pd.DataFrame(columns = ['name', 'height', 'weight'])

    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index = True)

    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index = True)

    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index = True)

    return extracted_data

def tranform(data):
    #convert inches to meters for height
    data['height'] = round(data.height * 0.0254, 2)

    # convert pounds to kilograms and round two 2 decimal places
    data['weight'] = round(data.weight * 0.45359237, 2)

    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)


def log_progress(message):

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as f:
        f.write(timestamp + ':' + message + '\n')


log_progress("ETL Started")

log_progress("Extraction Started")
extracted_data = extract()
log_progress("Extraction Completed")

log_progress("Transformation Started")
transformed_data = tranform(extracted_data)
print("Transformed Data:")
print(transformed_data)
log_progress("Transformation Completed")

log_progress("Load Started")
load_data(target_file, transformed_data)
log_progress("Load Completed")

log_progress("ETL Completed")