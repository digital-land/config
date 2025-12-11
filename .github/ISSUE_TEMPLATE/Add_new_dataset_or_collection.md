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
- [ ] Data Manager reviews the dataset, specification and confirm this is ready to be added
- [ ] Create the Collection and Pipeline created in the Config Repo (only if a new collection)
- [ ] Add the initial endpoint for the dataset using the add-data process
- [ ] Update the specification to include the collection in the Markdown file (this adds it to Airflow)
- [ ] Confirm the new collection is visible in Airflow
- [ ] Run Airflow dag in development and validate a successful run
- [ ] Share the collection results with Data Design for accuracy checks
- [ ] Collection should then run overnight in production
