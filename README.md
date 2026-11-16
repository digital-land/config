# Config
The authoritative source of collection and pipeline configuration.

This repository was created from the collection and pipeline csv
files of the original collection repositories.

Each subirectory of both collection and pipeline corresponds
to the original source collection repo. 

For example these [Historic England pipeline files](https://github.com/digital-land/config/tree/main/pipeline/historic-england)
are a copy from [the original Historic England collector repository](https://github.com/digital-land/historic-england-collection/tree/main/pipeline)

The headers in the csv files were updated to bring in line
with the relevant specifiation. 

For example the headers in any combine.csv should correspond
to the fields list in colmun.md. 

https://github.com/digital-land/specification/blob/main/content/dataset/combine.csv

## :warning: Known files not included
There may be reasons why git is inappropriate to store files for the configuration.
This is probably because they are too large. Below is a list of these files.
They are instead stored in the s3 bucket. These will need to be manually updated
by someone with acess.

the files below have been added to the git ignore

List of files:
- pipeline/title-boundary/lookup.csv

## Adding data from the command line

Most data is added through the Manage service, which commits each confirmed submission to the shared `config-manager-update` branch. The evening pipeline then merges that branch into `main` through a "Manage Service Update" pull request. When Manage isn't available, you can add an endpoint from your own machine with `make add-data` and commit it to the same branch, so your change goes through exactly the same route.

For the detail on preparing the input file, column mappings and edge cases, see [Add an endpoint](https://digital-land.github.io/technical-documentation/data-operations-manual/How-To-Guides/Adding/Add-an-endpoint/) in the data operations manual.

### One-time setup

1. Clone this repository and create a Python virtual environment in it (CI uses Python 3.9):

   ```
   python3.9 -m venv .venv
   ```

2. Install GDAL (`brew install gdal` on a Mac).

### 1. Get onto the shared branch

```
git fetch origin
git switch config-manager-update
git pull
```

If `config-manager-update` doesn't exist (it is deleted after each merge into `main`), create it from `main`:

```
git switch -c config-manager-update origin/main
```

Always work on this branch rather than `main`. It may already hold new entity numbers that haven't reached `main` yet, and starting from `main` can hand out the same numbers twice.

### 2. Install and download the collection's files

```
source .venv/bin/activate
make init COLLECTION=<collection>
```

This installs the Python packages (including the latest `digital-land` from `main`) and downloads the specification, the organisation list and the collection's logs. Run it again whenever you switch to a different collection.

### 3. Create `import.csv`

Create `import.csv` in the root of the repository (git ignores it). It must contain **one row only**: the command checks every row but only adds the last one.

```
organisation,endpoint-url,documentation-url,start-date,pipelines,plugin,licence
local-authority:XXX,https://example.gov.uk/data.csv,https://example.gov.uk/page-linking-to-it/,,plan-timetable,,ogl3
```

- `pipelines`: the dataset; separate several with `;`.
- `start-date` and `plugin`: usually blank.
- Add a `parameters` column only if the endpoint needs one.

If the data needs extra column mappings, add them to `pipeline/<collection>/column.csv` before running the command.

### 4. Run the command

```
make add-data COLLECTION=<collection> INPUT_CSV=import.csv
```

It downloads from the endpoint, runs the pipeline and prints a column summary, an issue summary and an entity summary, then asks:

| Prompt | Answer |
| --- | --- |
| Continue processing this resource? | `yes` |
| Assign entities for this resource? (only if there are new entities) | `yes` |
| Save changes made in this session? | `yes` if the checks below pass, otherwise `no` |
| Retire any of these existing endpoints? | `yes`, then `yes` only for the endpoints the new one replaces |
| View an updated dataset? | `no` (it downloads the whole dataset) |

### 5. Check what Manage would have checked

The command doesn't stop on problems the way Manage does, so check these yourself before saving:

- **Issues.** Look up each issue type in the issue summary in the specification's [issue-type.csv](https://github.com/digital-land/specification/blob/main/specification/issue-type.csv). If any has severity `critical`, don't save.
- **Entities on the platform missing from the new data.** The entity summary warns if any entities this organisation already has for the dataset aren't in the new resource. Check this is expected before retiring the old endpoint.
- **Duplicates.** If the organisation already has data for this dataset under different references, add redirects to `pipeline/<collection>/old-entity.csv`.
- **Entity ranges.** If the provider isn't the organisation that should own the new entities, or the dataset uses entity ranges, add a row to `pipeline/<collection>/entity-organisation.csv`.
- **Endpoint already exists.** If the command refuses with "duplicate endpoint", edit the existing row in `collection/<collection>/source.csv` instead (for example its `pipelines` or `documentation-url`).

### 6. Check the diff

```
git diff --stat
git diff
```

You should only see appended rows in `endpoint.csv`, `source.csv` and (if there were new entities) `lookup.csv`, plus `end-date` set on any endpoint and source you retired. If any existing rows have changed, discard those changes and keep only the new rows.

### 7. Commit and push

Check whether anyone has pushed changes to the same collection since you started:

```
git fetch origin
git log --oneline HEAD..origin/config-manager-update -- collection/<collection> pipeline/<collection>
```

If that prints nothing, commit and push:

```
git add collection/<collection> pipeline/<collection>
git commit -m "add <dataset> <organisation>"
git pull --rebase
git push origin config-manager-update
```

If it prints any commits, discard your changes (`git checkout -- collection/<collection> pipeline/<collection>`), run `git pull`, and run `make add-data` again, so any new entity numbers are assigned on top of theirs.

Your commit is merged into `main` with the rest of the branch, and the next nightly collection picks it up.
