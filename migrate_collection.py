import os
import shutil
import subprocess
import tempfile
import argparse

def clone_repo(url, temp_dir):
    """Clones the repository to a temporary directory."""
    print(f"Cloning repository: {url} into {temp_dir}")
    subprocess.check_call(['git', 'clone', url, temp_dir])

def copy_files(src_dir, dest_dir, filenames=None):
    """Copies specified files from source to destination directory. If filenames is None, copy all files."""
    print(f"Copying files from {src_dir} to {dest_dir}")
    if filenames is None:  # Copy all files and directories in the src_dir
        for item in os.listdir(src_dir):
            s = os.path.join(src_dir, item)
            d = os.path.join(dest_dir, item)
            print(f"Copying {s} to {d}")
            if os.path.isdir(s):
                shutil.copytree(s, d, dirs_exist_ok=True)
            else:
                shutil.copy2(s, d)
    else:  # Copy only specified files
        for filename in filenames:
            src_path = os.path.join(src_dir, filename)
            if os.path.exists(src_path):
                print(f"Copying file {src_path} to {dest_dir}")
                shutil.copy(src_path, dest_dir)
            else:
                print(f"File not found: {src_path}")

def main(collection_name, repo_url):
    temp_dir = tempfile.mkdtemp()
    try:
        # Clone the repository
        clone_repo(repo_url, temp_dir)
        
        # Handling collection folder
        collection_path = os.path.join(temp_dir, "collection")
        print(f"Looking for collection at: {collection_path}")
        handle_collection_or_pipeline(collection_path, "collection", ["endpoint.csv", "source.csv", "old-resource.csv"])
        
        # Handling pipeline folder
        pipeline_path = os.path.join(temp_dir, "pipeline")
        print(f"Looking for pipeline at: {pipeline_path}")
        pipeline_files = ["column.csv", "combine.csv", "concat.csv", "convert.csv", "default-value.csv", "default.csv", "filter.csv", "lookup.csv", "patch.csv", "skip.csv", "transform.csv"]
        handle_collection_or_pipeline(pipeline_path, "pipeline", pipeline_files)
        
    finally:
        # Cleanup: Remove the cloned repository directory
        shutil.rmtree(temp_dir)

def handle_collection_or_pipeline(path, folder_type, filenames):
    """Handles copying files from either collection or pipeline folder."""
    if os.path.exists(path):
        # Destination directory path
        dest_dir = os.path.join("./", folder_type, collection_name)
        print(f"Destination directory for {folder_type}: {dest_dir}")
        os.makedirs(dest_dir, exist_ok=True)
        
        # Copy the specified files
        copy_files(path, dest_dir, filenames)
        
        # For collection, also check for the "log" folder and copy if exists
        if folder_type == "collection":
            log_path = os.path.join(path, "log")
            if os.path.exists(log_path):
                log_dest_dir = os.path.join(dest_dir, "log")
                os.makedirs(log_dest_dir, exist_ok=True)
                print(f"Found log directory: {log_path}, copying to {log_dest_dir}")
                copy_files(log_path, log_dest_dir)  # Copy all contents of the log directory
                print("Log files copied successfully.")
        print(f"{folder_type.capitalize()} files copied successfully.")
    else:
        print(f"{folder_type.capitalize()} '{collection_name}' not found in the repository.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy files from a GitHub repository collection, including log files and pipeline files.")
    parser.add_argument("collection_name", type=str, help="The name of the collection")
    parser.add_argument("repo_url", type=str, help="The URL of the GitHub repository")

    args = parser.parse_args()

    collection_name = args.collection_name  # This makes collection_name available in handle_collection_or_pipeline
    main(args.collection_name, args.repo_url)