.PHONY: \
	collect\
	collection\
	commit-collection\
	clobber-today

ifeq ($(COLLECTION_CONFIG_URL),)
COLLECTION_CONFIG_URL=$(CONFIG_URL)collection/$(COLLECTION_NAME)/
endif

ifeq ($(COLLECTION_DIR),)
COLLECTION_DIR=collection/
endif

ifeq ($(RESOURCE_DIR),)
RESOURCE_DIR=$(COLLECTION_DIR)resource/
endif

ifeq ($(DATASTORE_URL),)
DATASTORE_URL=https://files.planning.data.gov.uk/
endif

ifeq ($(REGENERATE_LOG_OVERRIDE),True)
REFILL_TODAYS_LOGS=false
else
REFILL_TODAYS_LOGS=true
endif

ifeq ($(INCREMENTAL_LOADING_OVERRIDE),)
INCREMENTAL_LOADING_OVERRIDE=false
endif


# data sources
SOURCE_CSV=$(COLLECTION_DIR)source.csv
ENDPOINT_CSV=$(COLLECTION_DIR)endpoint.csv
OLD_RESOURCE_CSV=$(COLLECTION_DIR)old-resource.csv

ifeq ($(COLLECTION_CONFIG_FILES),)
COLLECTION_CONFIG_FILES=\
	$(SOURCE_CSV)\
	$(ENDPOINT_CSV)\
	$(OLD_RESOURCE_CSV)
endif

# collection log
LOG_DIR=$(COLLECTION_DIR)log/
LOG_FILES_TODAY:=$(LOG_DIR)$(shell date +%Y-%m-%d)/

# collection index
COLLECTION_INDEX=\
	$(COLLECTION_DIR)/log.csv\
	$(COLLECTION_DIR)/resource.csv

# collection URL
ifneq ($(COLLECTION),)
COLLECTION_URL=\
	$(DATASTORE_URL)$(COLLECTION)-collection/collection
else
COLLECTION_URL=\
	$(DATASTORE_URL)$(REPOSITORY)/collection
endif

init::
ifeq ($(COLLECTION_DATASET_BUCKET_NAME),)
	$(eval LOG_STATUS_CODE := $(shell curl -I -o /dev/null -s -w "%{http_code}" '$(COLLECTION_URL)/log.csv'))
	$(eval RESOURCE_STATUS_CODE = $(shell curl -I -o /dev/null -s -w "%{http_code}" '$(COLLECTION_URL)/resource.csv'))
	@if [ $(LOG_STATUS_CODE) -ne 403 ] && [ $(RESOURCE_STATUS_CODE) -ne 403 ]; then \
		echo 'Downloading log.csv and resource.csv from $(COLLECTION_URL)'; \
		curl -qfsL '$(COLLECTION_URL)/log.csv' > $(COLLECTION_DIR)log.csv; \
		curl -qfsL '$(COLLECTION_URL)/resource.csv' > $(COLLECTION_DIR)resource.csv; \
	else \
		echo 'Unable to locate log.csv and resource.csv' ;\
	fi
else ifeq ($(REGENERATE_LOG_OVERRIDE),True)
	echo 'Syncing log files to local';
	aws s3 sync s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(LOG_DIR) $(LOG_DIR) --only-show-errors;
else
	echo 'Downloading log.csv and resource.csv from s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/collection/'
	@if aws s3 ls s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/collection/log.csv > /dev/null 2>&1; then \
		aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/collection/log.csv collection/log.csv; \
	else \
		echo "Could not download log.csv from S3"; \
	fi
	@if aws s3 ls s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/collection/resource.csv > /dev/null 2>&1; then \
		aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/collection/resource.csv collection/resource.csv; \
	else \
		echo "Could not download resource.csv from S3"; \
	fi
endif

first-pass:: collect

second-pass:: collection

collect:: $(COLLECTION_CONFIG_FILES)
	@mkdir -p $(RESOURCE_DIR)
	digital-land ${DIGITAL_LAND_OPTS} collect $(ENDPOINT_CSV) --collection-dir $(COLLECTION_DIR) --refill-todays-logs $(REFILL_TODAYS_LOGS)

collection::
	digital-land ${DIGITAL_LAND_OPTS} collection-save-csv --collection-dir $(COLLECTION_DIR) --refill-todays-logs $(REFILL_TODAYS_LOGS)

clobber-today::
	rm -rf $(LOG_FILES_TODAY) $(COLLECTION_INDEX)

makerules::
	curl -qfsL '$(MAKERULES_URL)collection.mk' > makerules/collection.mk

load-resources::
	aws s3 sync s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(RESOURCE_DIR) $(RESOURCE_DIR) --no-progress

load-logs::
	aws s3 sync s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(COLLECTION_DIR)log $(COLLECTION_DIR)log --no-progress

detect-new-resources::
	aws s3 sync $(RESOURCE_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(RESOURCE_DIR) --dryrun --size-only | { grep -oP 'resource/\K[a-f0-9]+' || true; } > new_resources.txt
	
save-resources::
ifeq ($(INCREMENTAL_LOADING_OVERRIDE),True)
	aws s3 sync $(RESOURCE_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(RESOURCE_DIR) --no-progress
else
	aws s3 sync $(RESOURCE_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(RESOURCE_DIR) --size-only --no-progress
endif

save-logs::
	aws s3 sync $(COLLECTION_DIR)log s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(COLLECTION_DIR)log --no-progress

# if incremental loading is enabled than we still need to copy over the log and resource files
save-collection-log-resource::
	aws s3 cp $(COLLECTION_DIR)log.csv s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(COLLECTION_DIR) --no-progress
	aws s3 cp $(COLLECTION_DIR)resource.csv s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(COLLECTION_DIR) --no-progress

save-collection::
	aws s3 cp $(COLLECTION_DIR)log.csv s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(COLLECTION_DIR) --no-progress
	aws s3 cp $(COLLECTION_DIR)resource.csv s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(COLLECTION_DIR) --no-progress
	aws s3 cp $(COLLECTION_DIR)source.csv s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(COLLECTION_DIR) --no-progress
	aws s3 cp $(COLLECTION_DIR)endpoint.csv s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(COLLECTION_DIR) --no-progress
ifneq ($(wildcard $(COLLECTION_DIR)old-resource.csv),)
	aws s3 cp $(COLLECTION_DIR)old-resource.csv s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(COLLECTION_DIR) --no-progress
endif

collection/resource/%:
	@mkdir -p collection/resource/
ifeq ($(COLLECTION_DATASET_BUCKET_NAME),)
	curl -qfsL '$(DATASTORE_URL)$(REPOSITORY)/$(RESOURCE_DIR)$(notdir $@)' > $@
else
	aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(RESOURCE_DIR)$(notdir $@) $@ --no-progress
endif

collection/$(COLLECTION)/resource/%:
	@mkdir -p collection/$(COLLECTION)/resource
ifeq ($(COLLECTION_DATASET_BUCKET_NAME),)
	curl -qfsL '$(DATASTORE_URL)$(REPOSITORY)/$(RESOURCE_DIR)$(notdir $@)' > $@
else
	aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(RESOURCE_DIR)$(notdir $@) $@ --no-progress
endif

collection/%.csv:
	@mkdir -p $(COLLECTION_DIR)
ifeq ($(COLLECTION_DATASET_BUCKET_NAME),)
	curl -qfsL '$(COLLECTION_CONFIG_URL)$(notdir $@)?version=$(shell date +%s)' > $@
else
	aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/config/$(COLLECTION_DIR)$(COLLECTION_NAME)/$(notdir $@) $@ --no-progress
endif

config:: $(COLLECTION_CONFIG_FILES)

clean::
	rm -f $(COLLECTION_CONFIG_FILES)
