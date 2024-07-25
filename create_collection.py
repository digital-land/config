import os
import sys

def create_folders_and_files(base_dir, name, files_headers):
    """
    Create folders and files with headers for a specific project component.
    """
    dir_path = os.path.join(base_dir, name)
    os.makedirs(dir_path, exist_ok=True)
    
    for file_name, headers in files_headers.items():
        file_path = os.path.join(dir_path, file_name)
        try:
            with open(file_path, 'w') as file:
                file.write(headers + "\n")
        except IOError as e:
            return f"Error writing to {file_path}: {e}"

def create_project_structure(name):
    files_structure = {
        "./collection": {
            "endpoint.csv": "endpoint,endpoint-url,parameters,plugin,entry-date,start-date,end-date",
            "source.csv": "source,attribution,collection,documentation-url,endpoint,licence,organisation,pipelines,entry-date,start-date,end-date",
            "old-resource.csv": "old-resource,status,resource,notes"
        },
        "./pipeline": {
            "column.csv": "column,dataset,end-date,endpoint,entry-date,field,resource,start-date",
            "combine.csv": "dataset,end-date,endpoint,entry-date,field,resource,separator,start-date",
            "concat.csv": "end-date,entry-date,endpoint,field,fields,dataset,resource,separator,start-date",
            "convert.csv": "end-date,endpoint,entry-date,parameters,dataset,plugin,resource,start-date",
            "default-value.csv": "dataset,end-date,endpoint,entry-date,entry-number,field,resource,start-date,value",
            "default.csv": "dataset,default-field,end-date,endpoint,entry-date,entry-number,field,resource,start-date",
            "filter.csv": "dataset,end-date,endpoint,entry-date,entry-number,field,pattern,resource,start-date",
            "lookup.csv": "prefix,resource,entry-number,organisation,reference,entity",
            "old-entity.csv" : "old-entity,status,entity,notes,end-date,entry-date,start-date",
            "patch.csv": "dataset,end-date,endpoint,entry-date,entry-number,field,pattern,resource,start-date,value",
            "skip.csv": "dataset,end-date,endpoint,entry-date,entry-number,pattern,resource,start-date",
            "transform.csv": "dataset,end-date,endpoint,entry-date,entry-number,field,replacement-field,resource,start-date"
        }
    }

    for base_dir, headers in files_structure.items():
        result = create_folders_and_files(base_dir, name, headers)
        if result:
            return result

    return f"Created folders and files for '{name}' successfully."

def main(project_name):
    result = create_project_structure(project_name)
    print(result)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <project_name>")
    else:
        main(sys.argv[1])
