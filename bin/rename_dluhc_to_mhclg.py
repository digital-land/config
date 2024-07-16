# for collection in ....
# read in csv and change  org name
# write out csv again

#  repeat for pipline files

import os
import click
import csv
import logging

from pathlib import Path

def get_file_list(collection_dir,pipeline_dir):
    files = []

    # get collection files
    collection_dirs = [dir for dir in Path(collection_dir).iterdir() if dir.is_dir()]
    for dir  in collection_dirs:
        new_files = [file for file in dir.iterdir() if file.is_file() and file.stem == 'source']
        files += new_files
    
    pipeline_dirs = [dir for dir in Path(pipeline_dir).iterdir() if dir.is_dir()]
    for dir in pipeline_dirs:
        new_files = [file for file in dir.iterdir() if file.is_file()]
        files += new_files

    return files

def replace_org(file,old_org,new_org):
    # print(f'old:{old_org}')
    headers = []
    with file.open(mode='r',newline='') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        # print(f'fieldnames {reader.fieldnames}')
        
        new_rows=[]
        rows_changed=[]
        for row in reader:
            if 'organisation' in row.keys():
                row['organisation'] = row['organisation'].replace(old_org,new_org)
                new_rows.append(row)
                if new_org in row['organisation']:
                    rows_changed.append(row)
                
            else:
                new_rows.append(row)
        
    print(f'file {file} read, {len(rows_changed)} rows changed')

    with file.open(mode='w',newline='')as f:
        writer = csv.DictWriter(f,fieldnames=headers)
        writer.writeheader()
        writer.writerows(new_rows)



@click.command()
@click.option('--collection-dir',default='collection')
@click.option('--pipeline-dir',default='pipeline')
def replace_dluhc_mhclg(collection_dir,pipeline_dir):
    # Create an instance of the CollectionSync
    files = get_file_list(collection_dir,pipeline_dir)

    for file in files:
    # file = Path('collection/legislation/source.csv')
    # print (file in files)
        replace_org(file,'government-organisation:D1342','government-organisation:D1419')


if __name__ == '__main__':
    replace_dluhc_mhclg()