---
name: Add New Dataset or Collection
about: For adding new datasets and collections
title: '[New Dataset]'
labels: 'New Dataset'
assignees: ''

---
[Adding a new dataset and collection guidance](https://digital-land.github.io/technical-documentation/data-operations-manual/How-To-Guides/Adding/Add-a-new-dataset-and-collection/)

**Link to initial data to be added:**

**New Dataset Onboarding Checklist**

*Please tick off the steps as they are completed.*

- [ ] Data Design create the specification markdown in the [Specification repo](https://github.com/digital-land/specification/tree/main/content/dataset)
- [ ] Data Manager reviews the dataset, specification and confirm this is ready to be added - adds any advice around column mappings etc
- [ ] Create the Collection and Pipeline created in the Config Repo (only if a new collection)
- [ ] Update the specification to include the collection in the Markdown file (this adds it to Airflow)
- [ ] Add the dataset to the [provision rule](https://github.com/digital-land/specification/blob/main/content/provision-rule.csv) table
- [ ] Add the initial endpoint for the dataset using the add-data process
- [ ] Confirm the new collection is visible in Airflow
- [ ] Run Airflow dag in development and validate a successful run
- [ ] Share the collection results with Data Design for accuracy checks
- [ ] Collection should then run overnight in production
