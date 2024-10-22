# from root directory
    # for each folder in collection
        # if source.csv exists
            # check inside the source.csv file in the 'organisation' column
                # if the value includes '-eng', remove only that substring
        # else skip

    # for each folder in collection
        # if lookup.csv exists
            # chheck inside the file in the 'organisation' column
                # if the value inslices '-eng', remove only the substring   
            # else skip

import os
import pandas as pd

# these folders don't have organisation column filled, ignore else causes 'NoneType has no replace function error'
ignore_folders= ['document', 'ownership-status', 'site-category']

def process_csv(file_path):
    df = pd.read_csv(file_path)
    
    if 'organisation' in df.columns:
        # Remove '-eng' from 'organisation' column
        # df['organisation'] = df['organisation'].astype(str).str.replace('-eng', '', regex=False) 

        df['organisation'] = df['organisation'].apply(
            lambda x: str(x).replace('-eng', '') if pd.notna(x) and x != '' and str(x).lower() != 'nan' else x
        )
        
        # Save the modified DataFrame back to the CSV
        df.to_csv(file_path, index=False)
        print(f"Processed: {file_path}")
    else:
        print(f"Skipped (no 'organisation' column): {file_path}")

def process_directory(root_directory):
    # Process collection directory
    collection_dir = os.path.join(root_directory, 'collection' )
    if os.path.exists(collection_dir):
        print(collection_dir)
        print(f"Processing {collection_dir}...")
        for folder_name in os.listdir(collection_dir):

            if folder_name in ignore_folders:
                continue
            
            folder_path = os.path.join(collection_dir, folder_name)
            if os.path.isdir(folder_path):
                source_csv = os.path.join(folder_path, 'source.csv')
                if os.path.exists(source_csv):
                    process_csv(source_csv)
                else:
                    print(f"Skipped: {source_csv} does not exist")

    # Process pipeline directory
    pipeline_dir = os.path.join(root_directory, 'pipeline')
    if os.path.exists(pipeline_dir):
        print(pipeline_dir)
        print(f"Processing {pipeline_dir}...")
        for folder_name in os.listdir(pipeline_dir):
            folder_path = os.path.join(pipeline_dir, folder_name)
            if os.path.isdir(folder_path):
                lookup_csv = os.path.join(folder_path, 'lookup.csv')
                if os.path.exists(lookup_csv):
                    process_csv(lookup_csv)
                else:
                    print(f"Skipped: {lookup_csv} does not exist")

root_directory = '.'
process_directory(root_directory)
