import os
import sys

# Define column mappings (and their ordering) for collection and pipeline CSVs
COLUMN_MAPPINGS = {
    "collection": {
        "endpoint.csv": "endpoint,endpoint-url,parameters,plugin,entry-date,start-date,end-date",
        "source.csv": "source,attribution,collection,documentation-url,endpoint,licence,organisation,pipelines,entry-date,start-date,end-date",
        "old-resource.csv": "old-resource,status,resource,notes"
    },
    "pipeline": {
        "column.csv": "dataset,endpoint,resource,column,field,start-date,end-date,entry-date",
        "combine.csv": "dataset,endpoint,field,separator,entry-date,start-date,end-date,resource",
        "concat.csv": "dataset,resource,field,fields,separator,entry-date,start-date,end-date,endpoint,prepend,append",
        "convert.csv": "dataset,resource,plugin,start-date,end-date,entry-date,endpoint,parameters",
        "default-value.csv": "dataset,end-date,endpoint,entry-date,entry-number,field,resource,start-date,value",
        "default.csv": "dataset,resource,field,default-field,entry-date,start-date,end-date,entry-number,endpoint",
        "entity-organisation.csv": "dataset,entity-minimum,entity-maximum,organisation",
        "expect.csv": "datasets,organisations,operation,parameters,name,description,notes,severity,responsibility,end-date,entry-date,start-date",
        "filter.csv": "dataset,end-date,endpoint,entry-date,entry-number,field,pattern,resource,start-date",
        "lookup.csv": "prefix,resource,endpoint,entry-number,organisation,reference,entity,entry-date,start-date,end-date",
        "old-entity.csv": "old-entity,status,entity,notes,end-date,entry-date,start-date",
        "patch.csv": "dataset,resource,field,pattern,value,entry-number,start-date,end-date,entry-date,endpoint",
        "skip.csv": "dataset,resource,pattern,entry-number,start-date,end-date,entry-date,endpoint",
        "transform.csv": "dataset,field,replacement-field,entry-number,resource,start-date,end-date,entry-date,endpoint"
    }
}

def create_folders_and_files(base_dir, name, files_headers):
    """
    Create folders and files with headers for a specific project component.
    """
    dir_path = os.path.join(base_dir, name)
    os.makedirs(dir_path, exist_ok=True)
    
    for file_name, headers in files_headers.items():
        file_path = os.path.join(dir_path, file_name)
        try:
            with open(file_path, 'w', newline='') as file:
                file.write(headers + "\r\n")
        except IOError as e:
            return f"Error writing to {file_path}: {e}"

def create_project_structure(name):
    files_structure = {
        "./collection": COLUMN_MAPPINGS["collection"],
        "./pipeline": COLUMN_MAPPINGS["pipeline"]
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
