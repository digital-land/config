#! /usr/bin/env python

import pathlib, os, glob, csv, requests, frontmatter, shutil

github_url = "https://raw.githubusercontent.com/digital-land/specification/main/content/dataset"

if __name__ == "__main__":

    current_dir = pathlib.Path(__file__).parent.parent.absolute()
    pipeline_dir = os.path.join(current_dir, "pipeline")
    for file in glob.glob(f"{pipeline_dir}/**/*.csv", recursive=True):
        file_path = pathlib.Path(file)
        parent_path = file_path.parent
        pipeline_name = parent_path.parts[-1]
        file_name = file_path.stem
        extension = file_path.suffix

        spec_url = f"{github_url}/{file_name}.md"

        resp = requests.get(spec_url)
        from io import StringIO
        with StringIO(resp.text) as f:
            fm = frontmatter.load(f)
            field_array = fm["fields"]

        fields = []
        for field in field_array:
            fields.append(field["field"])

        temp_outfile = os.path.join(parent_path, f"{file_name}_temp_{extension}")


        print(80 * "*")
        print("file_path", file_path)
        print("parent_dir", parent_path)
        print("file_name", file_name)
        print("extension", extension)
        print("pipeline name", pipeline_name)
        print("fields from specification", fields)
        print("temp out file", temp_outfile)

        output_rows = []
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                dataset = row.get("dataset", None)
                if dataset is not None and dataset == pipeline_name:
                    output_rows.append(row)

        for out in output_rows:
            for field in fields:
                if field not in out:
                    out[field] = ""

        try:
            with open(temp_outfile, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                for row in output_rows:
                    writer.writerow(row)
        except Exception as e:
            print(e)

        print("moving", temp_outfile, file_path)
        shutil.move(temp_outfile, file_path)

