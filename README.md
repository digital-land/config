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

Most data is added through the Manage service, which commits each confirmed submission to the shared `config-manager-update` branch. The evening pipeline then merges that branch into `main` through a "Manage Service Update" pull request.

When Manage isn't available, `make manage-check` runs Manage's own checks on your machine and writes the same rows to the same branch, so your change follows exactly the same route. It runs async-request-backend's Check and Add data code, applies Manage's rule for what must be fixed, asks the questions Manage asks, and writes the rows with the same code Manage uses (`bin/add_data.py`).

### One-time setup

1. Clone this repository and create a Python virtual environment in it (CI uses Python 3.9):

   ```
   python3.9 -m venv .venv
   ```

2. Install GDAL and Python 3.13 (`brew install gdal python@3.13` on a Mac). The tool runs in its own Python 3.13 environment under `var/manage/`, which `make manage-check` creates the first time you run it. To use a different Python 3.10 or later, add `MANAGE_PYTHON_BIN=/path/to/python3.x` to the command.

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

If the branch exists but has no `bin/manage_check.py`, it was created before the tool was added. Bring it up to date with `git merge origin/main` and push.

Only one person should add data at a time. The tool refuses to start if your branch is behind GitHub, and refuses to write if someone pushes while it is running, but it can't see changes that someone else hasn't pushed yet.

### 2. Download the collection's files

```
source .venv/bin/activate
make init COLLECTION=<collection>
```

This downloads the specification, the organisation list and the collection's logs. Run it again whenever you switch to a different collection.

If the data needs extra column mappings, add them to `pipeline/<collection>/column.csv` now. The tool reads config from your checkout, so it uses them.

### 3. Run the check

```
make manage-check COLLECTION=<collection> DATASET=<dataset> ORGANISATION=<organisation> ENDPOINT_URL='<endpoint url>' DOCUMENTATION_URL='<documentation url>'
```

For example:

```
make manage-check COLLECTION=local-plan DATASET=plan-timetable ORGANISATION=local-authority:WRL ENDPOINT_URL='https://www.wirral.gov.uk/files/wbc-plan-timetable-october2026-v2.csv/download?inline' DOCUMENTATION_URL='https://www.wirral.gov.uk/planning-and-building/local-plans-and-planning-policy/wirrals-development-plan/local-plan-timetable'
```

Keep the single quotes around the URLs. The licence defaults to `ogl3` and the start date to today; to change them, add `MANAGE_CHECK_FLAGS='--licence <licence> --start-date YYYY-MM-DD'`.

It works through four stages:

1. **Fetch** downloads the endpoint, trying the ArcGIS and WFS plugins if a plain download fails.
2. **Check** prints what must be fixed and what should be fixed. If anything must be fixed (a missing mandatory column, or an issue with severity `critical`), it stops and writes nothing. Any entity numbers printed here are provisional.
3. **Add data** prints the new and existing entities, possible duplicates, redirects, and whether the endpoint and source are new. It also says whether the documentation page links to the endpoint. This is information only: if it doesn't, the documentation URL is probably wrong and is worth fixing, but the data can still be added. The log lines say files were downloaded from GitHub; they are actually read from your checkout.
4. **Write** asks the questions below, then writes the rows.

It also stops without writing if the organisation code doesn't exist, if the organisation isn't listed as a provider of the dataset in `specification/provision.csv`, or if the endpoint can't be fetched. Everything apart from the rows it writes goes in `var/manage/runs/`, including the Check and Add data responses as JSON.

### 4. Answer the questions

| Question | When it's asked | Answer |
| --- | --- | --- |
| You are on `<branch>`, not config-manager-update. Continue anyway? | You aren't on `config-manager-update` | `N`, then switch branch. Answer `y` only to try the tool out, as the branch checks are then skipped. |
| Retire existing endpoint `<url>` (start `<date>`)? | Once for each active endpoint the organisation already has for this dataset | `y` if the new endpoint replaces it, otherwise `N`. Endpoints that are already retired are listed but not asked about. If you retire an endpoint and the run also creates new entities, it warns you: a replacement normally creates none, so stop and check the organisation hasn't changed its references. |
| Is `<organisation>` the authoritative source for this data? | Only when there are new entities | `y` if the organisation is publishing its own data, for example a council's own plan timetable. `n` if it is publishing data about another organisation's area; the data will then count as "some" rather than "authoritative". |
| Write these changes to collection/`<collection>`/ and pipeline/`<collection>`/? | Always, last | `y` to write the rows. `N` writes nothing. |

### 5. Check the diff

```
git diff --stat
git diff
```

You should only see appended rows in `endpoint.csv`, `source.csv` and, if there were new entities, `lookup.csv` and `entity-organisation.csv`, plus `end-date` set on any endpoint and source you retired.

### 6. Commit and push straight away

```
git add collection/<collection> pipeline/<collection>
git commit -m "add <dataset> <organisation>"
git push origin config-manager-update
```

Push straight away so the next person starts from your rows. If the push is rejected because someone else has pushed, don't rebase or merge; new entity numbers could clash. Throw your commit away, update, and run the check again:

```
git fetch origin
git reset --hard origin/config-manager-update
```

Your commit is merged into `main` with the rest of the branch, and the next nightly collection picks it up.

### What the tool doesn't do yet

Manage can do these; the tool can't:

- Name a different organisation as the authoritative source. Add the row to `pipeline/<collection>/entity-organisation.csv` by hand.
- Unretire an endpoint. Clear its `end-date` in both `collection/<collection>/endpoint.csv` and `source.csv` by hand.
- Choose which possible duplicates to redirect, or leave out some new entities. The tool lists possible duplicates but only writes the redirects async creates automatically; add any others to `pipeline/<collection>/old-entity.csv`.
- The column mapping screen and the geometry type option. Edit `column.csv` before running instead.
- Page size settings for ArcGIS endpoints, comparing each entity's fields with the platform, the map preview, and re-adding a flagged resource.
- An admin override for anything that must be fixed.

For anything else, the older `make add-data` route is described in [Add an endpoint](https://digital-land.github.io/technical-documentation/data-operations-manual/How-To-Guides/Adding/Add-an-endpoint/) in the data operations manual.
