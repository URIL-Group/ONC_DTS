from pathlib import Path
from typing import Dict, Any, Union
from base64 import b64decode
from datetime import datetime
from dateutil import parser

import json
import csv
import numpy as np


def process_folder(input_folder, csv_file_path, trim, raw):
    
    folder = Path(input_folder)
    files = sorted(folder.glob("*.json")) 

    print(f"Total JSON files: {len(files)}") #has only been tested up to 416 files. Not sure how it will do with any more

    #for channel 1
    all_dates_1 = []
    all_columns_1 = []

    #for channel 2
    all_dates_2 = []
    all_columns_2 = []

    for idx, file_path in enumerate(files):
        try:
            
            rows = read_json_file(file_path, include_raw=raw, trim=trim)
            
            date = rows['date_time']
            distance = rows['distance']
            temp = rows['temp_data']
            channel=rows['channel']

            if channel == 1:
                all_dates_1.append(date)
                all_columns_1.append([distance, temp])
                
            else:  # channel 2
                all_dates_2.append(date)
                all_columns_2.append([distance, temp])

        except Exception as e:
            print(f"Error with {file_path.name}: {e}")

    
    # Write Channel 1
    csv_creator(all_dates_1, all_columns_1, f"{csv_file_path}_1.csv")
    print(f"Created {csv_file_path}_1.csv with {len(all_dates_1)} sets")

    # Write Channel 2
    csv_creator(all_dates_2, all_columns_2, f"{csv_file_path}_2.csv")
    print(f"Created {csv_file_path}_2.csv with {len(all_dates_2)} sets")


def read_json_file(file_path: Union[str, Path],
                   include_raw,
                   trim,
                   channel_points: Dict[int, int] = {1: 2206, 2: 1561}) -> Dict[str, Any]:


    # Load the JSON data
    with open(file_path, 'r') as f:
        json_data = json.load(f)

    out = {}

    # Extract the date and the JSON file
    metadata = {
        'channel': json_data['Resp']['processed data'].get('forward channel', 0) + 1,
        'dz':json_data['Resp']['processed data']['resampled forward raw data']['dz'],
        'first_external_point': json_data['Resp']['processed data']['resampled temperature data']['first external point'],
        'datetime': json_data['Resp']['processed data'].get('measurement start time', None),
    }
    
    date_time=metadata['datetime']
    channel=metadata['channel']
    out['channel']=channel
       
    metadata['n_external_points'] = channel_points[metadata['channel']]
    metadata['external_length'] = metadata['n_external_points'] * metadata['dz']
    metadata['total_length'] = metadata['external_length'] + metadata['first_external_point'] * metadata['dz']

    new_Date=parser.isoparse(date_time)  #parses the string
    formatted = new_Date.strftime("%#m/%#d/%Y %H:%M:%S") #formats the date 

    out['date_time'] = formatted

    # Extract temperature data and convert from Kelvin to Celsius
    temp_data = np.frombuffer(
        b64decode(json_data['Resp']['processed data']['resampled temperature data']['signal']['Data']), 
        dtype='<f4'
    ) - 273.15  # Convert from K to °C
    
    
    # Calculate the distance array based on first external point
    pt_from = metadata['first_external_point']
    pt_to = pt_from + channel_points[metadata['channel']]
    
    if trim:
        distance = (np.arange(pt_from, pt_to) - pt_from) * metadata['dz']
        out['temp_data'] = temp_data[pt_from:pt_to]
    else:
        distance = np.arange(0, len(temp_data)) * metadata['dz']
        #print(temp_data)
        out['temp_data'] = temp_data

    out['distance'] = distance
    
    if include_raw:
        # Extract raw data if available
        raw_data: Dict[str, np.ndarray] = {}
        
        # Process forward raw data if present
        if 'resampled forward raw data' in json_data['Resp']['processed data']:
            raw_fwd = np.frombuffer(
                b64decode(json_data['Resp']['processed data']['resampled forward raw data']['signal']['Data']), 
                dtype='<f4'
            )
            # Reshape if necessary - typically for multi-channel data
            if len(raw_fwd) > len(temp_data):
                # Determine number of channels from the JSON or use default of 2
                channels = json_data['Resp']['processed data'].get('number of channels', 2)
                raw_fwd = raw_fwd.reshape(channels, -1)
            raw_data['forward'] = raw_fwd
        
        # Process reverse raw data if present    
        if 'resampled reverse raw data' in json_data['Resp']['processed data']:
            raw_rev = np.frombuffer(
                b64decode(json_data['Resp']['processed data']['resampled reverse raw data']['signal']['Data']), 
                dtype='<f4'
            )
            # Reshape if necessary and maintain consistent shape with forward data
            if len(raw_rev) > len(distance) and 'forward' in raw_data:
                raw_rev = raw_rev.reshape(raw_data['forward'].shape[0], -1)
            raw_data['reverse'] = raw_rev
            
        #out['raw_data'] = raw_data    


            
    # Return structured dictionary with all extracted information
    return out

def csv_creator(all_dates, all_columns, csv_file_path):

   #max number of temperatures recorded 
    max_rows = max(len(columns[1]) for columns in all_columns)

    with open(csv_file_path, mode='w', newline='') as csv_file:
        
        writer = csv.writer(csv_file)

        # Row 1 is the headings
        header_row = ["Distance"]
        
        for date in all_dates:
            header_row.append(date)
            
        writer.writerow(header_row)

        # Write data rows
        for i in range(max_rows):
            row = []

            # First column: distance. columns[0] = distance
            #all_columns[0][0] is distance, which the length is 8
            if i < len(all_columns[0][0]): #first column first row
                row.append(all_columns[0][0][i])
                
            else:
                row.append("")  # Pad if i is more than distance (8)

            # Remaining columns: temperature from each dataset
            for columns in all_columns:
                row.append(columns[1][i] if i < len(columns[1]) else "") #column[1][i] is the ith temperature

            writer.writerow(row)

