## Data Template

**Ticket**
- Link: 

**Type**
- [ ] New data
- [ ] Data monitoring
- [ ] Data fix

**Data updated (list organisation and dataset):**
- Dataset:

**Expected outcome (if relevant):**
- [ ] New endpoint & source
- [ ] New lookups
- [ ] Extra endpoint config (e.g. column, concat)
- [ ] Retired old endpoint & source
- [ ] Retired old entities
- [ ] Retired old resource

**Additional information:**
- Any other relevant details or considerations.

**Requester's checklist:**
- [ ] Have checked if any old endpoints to retire
- [ ] Have validated endpoint (with check or endpoint checker)
- [ ] Have checked expected number of lookups
- [ ] Have checked for any geo duplicates (if CA data)
- [ ] Have updated entity-organisation.csv (if CA data)

**Reviewer's checklist:**
- [ ] Expected checks have been completed by PR requester
- [ ] Correct date format used in config files (YYYY-mm-dd)
- [ ] Number of new lookups is as expected from source data
- [ ] Spot checked that newly assigned entity numbers aren’t in use
