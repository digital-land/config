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
