---
name: Weird Data
about: Report unexpected, suspicious or incorrect data in a planning data
title: '[Weird Data] <dataset> - <brief description>'
labels: 'weird-data'
assignees: ''

---

### What did you find?
_Describe the weird data — what looks wrong, unexpected, or suspicious._

### Where is it?
_Dataset name, organisation, endpoint URL, or resource hash if known._

### How did you find it?
_e.g. manual review, automated check, user report, pipeline output_

### What were you expecting?
_What should the data look like?_

### What does it actually look like?
_Paste an example row, screenshot, or link to the data._

### Impact
- [ ] P0 critical - major data inconsistency affecting many records
- [ ] P1 high - affects core dataset functions or published outputs
- [ ] P2 medium - isolated issue, limited downstream impact
- [ ] P3 low - cosmetic or edge case

### Investigation checklist

- [ ] Find the entry in GitHub config — check git blame for any recent changes
- [ ] Find the endpoint the data is collected from and try accessing it directly
- [ ] Find the resources collected from this endpoint and check the resource log for errors
- [ ] Run the endpoint through the manage service — check whether the converted/transformed output looks correct
- [ ] Check the entities created from this data — are they being created as expected?
- [ ] Run `debug_resource_transformation.ipynb` (digital-land-python) with the latest resource hash to inspect what is being transformed
- [ ] Run the collection task with only this endpoint to isolate the issue (Tricky)
- [ ] Check S3 logs across each pipeline phase (Transform, Append) to see where data may be dropping off