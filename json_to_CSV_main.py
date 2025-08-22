from json_to_CSV import process_folder, read_json_file, csv_creator

# File paths
input_folder = "C:/Users/katie/Desktop/ONC Files/files/"
csv_file_path = "C:/Users/katie/Desktop/ONC Files/channel" #where you want it to save

raw = False #raw data doesn't go into the final CSV file
trim = True

process_folder(input_folder, csv_file_path, trim, raw)

#288 files a day at 5 minutes interval sampling
#1.15 GB of data per day for one csv file 
