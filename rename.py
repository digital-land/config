import os
import pandas as pd
import numpy as np

def process_csv(file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    
    # Check if the 'organisation' column exists
    if 'organisation' in df.columns:
        # Convert the column to string type, replacing NaN with empty string
        df['organisation'] = df['organisation'].fillna('').astype(str)
        
        # Check if any rows actually contain '-eng' before making changes
        rows_with_eng = df['organisation'].str.contains('-eng', na=False).any()
        
        if rows_with_eng:
            # Remove '-eng' from 'organisation' column
            df['organisation'] = df['organisation'].str.replace('-eng', '', regex=False)
            
            # Replace empty strings back to NaN if needed
            df['organisation'] = df['organisation'].replace('', np.nan)
            
            # Save the modified DataFrame back to the CSV only if changes were made
            df.to_csv(file_path, index=False)
            print(f"Processed (changes made): {file_path}")
        else:
            print(f"Skipped (no '-eng' found): {file_path}")
    else:
        print(f"Skipped (no 'organisation' column): {file_path}")

def process_directory(root_directory):
    try:
        # Process collection directory
        collection_dir = os.path.join(root_directory, 'collection')
        if os.path.exists(collection_dir):
            print("Processing collection directory...")
            for folder_name in os.listdir(collection_dir):
                folder_path = os.path.join(collection_dir, folder_name)
                if os.path.isdir(folder_path):
                    source_csv = os.path.join(folder_path, 'source.csv')
                    if os.path.exists(source_csv):
                        try:
                            process_csv(source_csv)
                        except Exception as e:
                            print(f"Error processing {source_csv}: {str(e)}")
                    else:
                        print(f"Skipped: {source_csv} does not exist")

        # Process pipeline directory
        pipeline_dir = os.path.join(root_directory, 'pipeline')
        if os.path.exists(pipeline_dir):
            print("Processing pipeline directory...")
            for folder_name in os.listdir(pipeline_dir):
                folder_path = os.path.join(pipeline_dir, folder_name)
                if os.path.isdir(folder_path):
                    lookup_csv = os.path.join(folder_path, 'lookup.csv')
                    if os.path.exists(lookup_csv):
                        try:
                            process_csv(lookup_csv)
                        except Exception as e:
                            print(f"Error processing {lookup_csv}: {str(e)}")
                    else:
                        print(f"Skipped: {lookup_csv} does not exist")
                        
    except Exception as e:
        print(f"Error during directory processing: {str(e)}")

if __name__ == "__main__":
    # Example usage:
    root_directory = '.'
    process_directory(root_directory)