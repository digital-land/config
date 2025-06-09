.PHONY: \
	transformed\
	dataset\
	commit-dataset

# data sources
ifeq ($(PIPELINE_CONFIG_URL),)
PIPELINE_CONFIG_URL=$(CONFIG_URL)pipeline/$(COLLECTION_NAME)/
endif

ifeq ($(COLLECTION_DIR),)
COLLECTION_DIR=collection/
endif

ifeq ($(PIPELINE_DIR),)
PIPELINE_DIR=pipeline/
endif

# collected resources
ifeq ($(RESOURCE_DIR),)
RESOURCE_DIR=$(COLLECTION_DIR)resource/
endif

ifeq ($(RESOURCE_FILES),)
RESOURCE_FILES:=$(wildcard $(RESOURCE_DIR)*)
endif

ifeq ($(FIXED_DIR),)
FIXED_DIR=fixed/
endif

ifeq ($(VAR_DIR),)
VAR_DIR=var/
endif

ifeq ($(CACHE_DIR),)
CACHE_DIR=$(VAR_DIR)cache/
endif

ifeq ($(TRANSFORMED_DIR),)
TRANSFORMED_DIR=transformed/
endif

ifeq ($(ISSUE_DIR),)
ISSUE_DIR=issue/
endif

ifeq ($(OUTPUT_LOG_DIR),)
OUTPUT_LOG_DIR=log/
endif

ifeq ($(PERFORMANCE_DIR),)
PERFORMANCE_DIR=performance/
endif

ifeq ($(OPERATIONAL_ISSUE_DIR),)
OPERATIONAL_ISSUE_DIR=$(PERFORMANCE_DIR)operational_issue/
endif

ifeq ($(COLUMN_FIELD_DIR),)
COLUMN_FIELD_DIR=$(VAR_DIR)column-field/
endif

ifeq ($(DATASET_RESOURCE_DIR),)
DATASET_RESOURCE_DIR=$(VAR_DIR)dataset-resource/
endif

ifeq ($(CONVERTED_RESOURCE_DIR),)
CONVERTED_RESOURCE_DIR=$(VAR_DIR)converted-resource/
endif

ifeq ($(DATASET_DIR),)
DATASET_DIR=dataset/
endif

ifeq ($(FLATTENED_DIR),)
FLATTENED_DIR=flattened/
endif

ifeq ($(PARQUET_DIR),)
PARQUET_DIR=data/
endif

ifeq ($(DATASET_DIRS),)
DATASET_DIRS=\
	$(TRANSFORMED_DIR)\
	$(COLUMN_FIELD_DIR)\
	$(DATASET_RESOURCE_DIR)\
	$(CONVERTED_RESOURCE_DIR)\
	$(ISSUE_DIR)\
	$(PERFORMANCE_DIR)\
	$(DATASET_DIR)\
	$(FLATTENED_DIR)
endif

ifeq ($(EXPECTATION_DIR),)
EXPECTATION_DIR = expectations/
endif

ifeq ($(PIPELINE_CONFIG_FILES),)
PIPELINE_CONFIG_FILES=\
	$(PIPELINE_DIR)column.csv\
	$(PIPELINE_DIR)combine.csv\
	$(PIPELINE_DIR)concat.csv\
	$(PIPELINE_DIR)convert.csv\
	$(PIPELINE_DIR)default.csv\
	$(PIPELINE_DIR)default-value.csv\
	$(PIPELINE_DIR)filter.csv\
	$(PIPELINE_DIR)lookup.csv\
	$(PIPELINE_DIR)old-entity.csv\
	$(PIPELINE_DIR)patch.csv\
	$(PIPELINE_DIR)skip.csv\
	$(PIPELINE_DIR)transform.csv\
	$(PIPELINE_DIR)entity-organisation.csv\
	$(PIPELINE_DIR)expect.csv
endif

ifeq ($(SPECIFICATION_DIR),)
SPECIFICATION_DIR = specification/
endif

define run-pipeline
	mkdir -p $(@D) $(ISSUE_DIR)$(notdir $(@D)) $(OPERATIONAL_ISSUE_DIR) $(OUTPUT_LOG_DIR) $(COLUMN_FIELD_DIR)$(notdir $(@D)) $(DATASET_RESOURCE_DIR)$(notdir $(@D)) $(CONVERTED_RESOURCE_DIR)$(notdir $(@D))
	digital-land ${DIGITAL_LAND_OPTS} --dataset $(notdir $(@D)) --pipeline-dir $(PIPELINE_DIR) $(DIGITAL_LAND_FLAGS) pipeline $(1) --issue-dir $(ISSUE_DIR)$(notdir $(@D)) --column-field-dir $(COLUMN_FIELD_DIR)$(notdir $(@D)) --dataset-resource-dir $(DATASET_RESOURCE_DIR)$(notdir $(@D)) --converted-resource-dir $(CONVERTED_RESOURCE_DIR)$(notdir $(@D)) --config-path $(CACHE_DIR)config.sqlite3 --organisation-path $(CACHE_DIR)organisation.csv $(PIPELINE_FLAGS) $< $@
endef

define build-dataset =
	mkdir -p $(@D)
	time digital-land ${DIGITAL_LAND_OPTS} --dataset $(notdir $(basename $@)) --pipeline-dir $(PIPELINE_DIR)  dataset-create --output-path $(basename $@).sqlite3 --organisation-path $(CACHE_DIR)organisation.csv --issue-dir $(ISSUE_DIR) --column-field-dir=$(COLUMN_FIELD_DIR) --dataset-resource-dir $(DATASET_RESOURCE_DIR) --resource-path $(COLLECTION_DIR)resource.csv --cache-dir $(CACHE_DIR) $ $(^)
	time datasette inspect $(basename $@).sqlite3 --inspect-file=$(basename $@).sqlite3.json
	time digital-land ${DIGITAL_LAND_OPTS} --dataset $(notdir $(basename $@)) --pipeline-dir $(PIPELINE_DIR) dataset-entries $(basename $@).sqlite3 $@
	mkdir -p $(FLATTENED_DIR)
	time digital-land ${DIGITAL_LAND_OPTS} --dataset $(notdir $(basename $@)) --pipeline-dir $(PIPELINE_DIR) dataset-entries-flattened $@ $(FLATTENED_DIR)
	md5sum $@ $(basename $@).sqlite3
	csvstack $(ISSUE_DIR)$(notdir $(basename $@))/*.csv > $(basename $@)-issue.csv
	time digital-land ${DIGITAL_LAND_OPTS} expectations-dataset-checkpoint --dataset $(notdir $(basename $@)) --file-path $(basename $@).sqlite3  --log-dir=$(OUTPUT_LOG_DIR) --configuration-path $(CACHE_DIR)config.sqlite3 --organisation-path $(CACHE_DIR)organisation.csv --specification-dir $(SPECIFICATION_DIR)
	time digital-land ${DIGITAL_LAND_OPTS} --dataset $(notdir $(basename $@)) operational-issue-save-csv --operational-issue-dir $(OPERATIONAL_ISSUE_DIR)
endef

define update-dataset =
	mkdir -p $(@D)
	time digital-land ${DIGITAL_LAND_OPTS} --dataset $(notdir $(basename $@)) --pipeline-dir $(PIPELINE_DIR)  dataset-update --output-path $(basename $@).sqlite3 --organisation-path $(CACHE_DIR)organisation.csv --issue-dir $(ISSUE_DIR) --column-field-dir=$(COLUMN_FIELD_DIR) --dataset-resource-dir $(DATASET_RESOURCE_DIR) --resource-path $(COLLECTION_DIR)resource.csv --cache-dir $(CACHE_DIR) $ $(^) --bucket-name $(COLLECTION_DATASET_BUCKET_NAME)
	time datasette inspect $(basename $@).sqlite3 --inspect-file=$(basename $@).sqlite3.json
	time digital-land ${DIGITAL_LAND_OPTS} --dataset $(notdir $(basename $@)) --pipeline-dir $(PIPELINE_DIR) dataset-entries $(basename $@).sqlite3 $@
	mkdir -p $(FLATTENED_DIR)
	time digital-land ${DIGITAL_LAND_OPTS} --dataset $(notdir $(basename $@)) --pipeline-dir $(PIPELINE_DIR) dataset-entries-flattened $@ $(FLATTENED_DIR)
	md5sum $@ $(basename $@).sqlite3
	# Get existing issue file from S3 (if it does not exist then do not error, hence "|| true" at the end)
	aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(ISSUE_DIR)/$(basename $@)-issue.csv $(basename $@)-issue.csv || true
	# Check if file does not exist or is empty (if empty cannot merge with newer issues)
	if [ -s $(basename $@)-issue.csv ]; then
		# Merge existing issues with new issues
		csvstack $(basename $@)-issue.csv $(ISSUE_DIR)/*.csv > $(basename $@)-issue-updated.csv
	else
		csvstack $(ISSUE_DIR)/*.csv > $(basename $@)-issue-updated.csv
	fi
	mv $(basename $@)-issue-updated.csv $(basename $@)-issue.csv
	time digital-land ${DIGITAL_LAND_OPTS} expectations-dataset-checkpoint --dataset $(notdir $(basename $@)) --file-path $(basename $@).sqlite3  --log-dir=$(OUTPUT_LOG_DIR) --configuration-path $(CACHE_DIR)config.sqlite3 --organisation-path $(CACHE_DIR)organisation.csv --specification-dir $(SPECIFICATION_DIR)
	time digital-land ${DIGITAL_LAND_OPTS} --dataset $(notdir $(basename $@)) operational-issue-save-csv --operational-issue-dir $(OPERATIONAL_ISSUE_DIR)
endef

collection::
	@if [ -f "state.json" ]; then \
		digital-land ${DIGITAL_LAND_OPTS} collection-pipeline-makerules --collection-dir $(COLLECTION_DIR) --specification-dir $(SPECIFICATION_DIR) --pipeline-dir $(PIPELINE_DIR) --resource-dir $(COLLECTION_DIR)resource/ --incremental-loading-override $(INCREMENTAL_LOADING_OVERRIDE) --state-path state.json > $(COLLECTION_DIR)pipeline.mk; \
	else \
		digital-land ${DIGITAL_LAND_OPTS} collection-pipeline-makerules --collection-dir $(COLLECTION_DIR) --specification-dir $(SPECIFICATION_DIR) --pipeline-dir $(PIPELINE_DIR) --resource-dir $(COLLECTION_DIR)resource/ --incremental-loading-override $(INCREMENTAL_LOADING_OVERRIDE) > $(COLLECTION_DIR)pipeline.mk; \
	fi

-include $(COLLECTION_DIR)pipeline.mk

# restart the make process to pick-up collected resource files
second-pass::
	@$(MAKE) --no-print-directory transformed dataset

GDAL := $(shell command -v ogr2ogr 2> /dev/null)
UNAME := $(shell uname)

init::
	pip install csvkit
ifndef GDAL
ifeq ($(UNAME),Darwin)
	$(error GDAL tools not found in PATH)
endif
	sudo add-apt-repository ppa:ubuntugis/ppa
	sudo apt-get update
	sudo apt-get install gdal-bin
endif
	pyproj sync --file uk_os_OSTN15_NTv2_OSGBtoETRS.tif -v
ifeq ($(UNAME),Linux)
	dpkg-query -W libsqlite3-mod-spatialite >/dev/null 2>&1 || sudo apt-get install libsqlite3-mod-spatialite
endif

clobber::
	rm -rf $(DATASET_DIRS)

clean::
	rm -rf ./$(VAR_DIR)

# local copy of the organisation dataset
# Download historic operational issue log data for relevant datasets
init:: $(CACHE_DIR)organisation.csv
ifeq ($(COLLECTION_DATASET_BUCKET_NAME),)
	@datasets=$$(awk -F , '$$2 == "$(COLLECTION_NAME)" {print $$4}' specification/dataset.csv); \
	for dataset in $$datasets; do \
		mkdir -p $(OPERATIONAL_ISSUE_DIR)$$dataset; \
		url="$(DATASTORE_URL)$(OPERATIONAL_ISSUE_DIR)$$dataset/operational-issue.csv"; \
		echo "Downloading operational issue log for $$dataset at url $$url";\
		status_code=$$(curl --write-out "%{http_code}" --head --silent --output /dev/null "$$url"); \
		if [ "$$status_code" -eq 200 ]; then \
			echo "Downloading file..."; \
			curl --silent --output "$(OPERATIONAL_ISSUE_DIR)$$dataset/operational-issue.csv" "$$url"; \
			echo "Log downloaded to $(OPERATIONAL_ISSUE_DIR)$$dataset/operational-issue.csv"; \
		else \
			echo "File not found at $$url"; \
		fi; \
	done
else
	@datasets=$$(awk -F , '$$2 == "$(COLLECTION_NAME)" {print $$4}' specification/dataset.csv); \
	for dataset in $$datasets; do \
		mkdir -p $(OPERATIONAL_ISSUE_DIR)$$dataset; \
		url="s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(OPERATIONAL_ISSUE_DIR)$$dataset/operational-issue.csv"; \
        if aws s3 ls $$url > /dev/null 2>&1; then \
            echo "File found at $$url, downloading..."; \
            aws s3 cp $$url $(OPERATIONAL_ISSUE_DIR)/$$dataset/operational-issue.csv --no-progress; \
        else \
            echo "File not found at $$url"; \
        fi; \
	done
endif

makerules::
	curl -qfsL '$(MAKERULES_URL)pipeline.mk' > makerules/pipeline.mk

save-transformed::
	@if [ -d "$(TRANSFORMED_DIR)" ]; then \
		aws s3 sync $(TRANSFORMED_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(TRANSFORMED_DIR) --no-progress; \
	fi
	@if [ -d "$(ISSUE_DIR)" ]; then \
		aws s3 sync $(ISSUE_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(ISSUE_DIR) --no-progress; \
	fi
	@if [ -d "$(COLUMN_FIELD_DIR)" ]; then \
		aws s3 sync $(COLUMN_FIELD_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(COLUMN_FIELD_DIR) --no-progress; \
	fi
	@if [ -d "$(DATASET_RESOURCE_DIR)" ]; then \
		aws s3 sync $(DATASET_RESOURCE_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(DATASET_RESOURCE_DIR) --no-progress; \
	fi
	@if [ -d "$(CONVERTED_RESOURCE_DIR)" ]; then \
		aws s3 sync $(CONVERTED_RESOURCE_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(CONVERTED_RESOURCE_DIR) --no-progress; \
	fi
	@if [ -d "$(OUTPUT_LOG_DIR)" ]; then \
		aws s3 sync $(OUTPUT_LOG_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(OUTPUT_LOG_DIR) --no-progress; \
	fi

save-dataset::
	@mkdir -p $(DATASET_DIR)
	aws s3 sync $(DATASET_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/$(DATASET_DIR) --no-progress
	@mkdir -p $(FLATTENED_DIR)
ifeq ($(HOISTED_COLLECTION_DATASET_BUCKET_NAME),digital-land-$(ENVIRONMENT)-collection-dataset-hoisted)
	aws s3 sync $(FLATTENED_DIR) s3://$(HOISTED_COLLECTION_DATASET_BUCKET_NAME)/data/ --no-progress
else
	aws s3 sync $(FLATTENED_DIR) s3://$(HOISTED_COLLECTION_DATASET_BUCKET_NAME)/dataset/ --no-progress --content-disposition attachment
endif

save-expectations::
	@mkdir -p $(OUTPUT_LOG_DIR)
	aws s3 sync $(OUTPUT_LOG_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(OUTPUT_LOG_DIR) --no-progress

save-performance::
	@mkdir -p $(PERFORMANCE_DIR)
	aws s3 sync $(PERFORMANCE_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(PERFORMANCE_DIR) --no-progress

save-tables-to-parquet:
	@mkdir -p $(PARQUET_DIR)
	aws s3 sync $(PARQUET_DIR) s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(PARQUET_DIR) --no-progress

# convert an individual resource
# .. this assumes conversion is the same for every dataset, but it may not be soon
var/converted/%.csv: collection/resource/%
	mkdir -p $(VAR_DIR)converted/
	digital-land ${DIGITAL_LAND_OPTS} convert $<

transformed::
	@mkdir -p $(TRANSFORMED_DIR)

metadata.json:
	echo "{}" > $@

datasette:	metadata.json
	datasette serve $(DATASET_DIR)/*.sqlite3 \
	--setting sql_time_limit_ms 5000 \
	--load-extension $(SPATIALITE_EXTENSION) \
	--metadata metadata.json

$(PIPELINE_DIR)%.csv:
	@mkdir -p $(PIPELINE_DIR)
ifeq ($(COLLECTION_DATASET_BUCKET_NAME),)
	curl -qfsL '$(PIPELINE_CONFIG_URL)$(notdir $@)?version=$(shell date +%s)' -o $@
else
	aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/config/$(PIPELINE_DIR)$(COLLECTION_NAME)/$(notdir $@) $@ --no-progress
endif

config:: $(PIPELINE_CONFIG_FILES)
ifeq ($(PIPELINE_CONFIG_FILES), .dummy)
	echo "pipeline_config_files are dummy not making config.sqlite" 
else
	mkdir -p $(CACHE_DIR)
	digital-land --pipeline-dir $(PIPELINE_DIR) config-create --config-path $(CACHE_DIR)config.sqlite3
	digital-land --pipeline-dir $(PIPELINE_DIR) config-load --config-path $(CACHE_DIR)config.sqlite3
endif

clean::
	rm -f $(PIPELINE_CONFIG_FILES)

state.json:
	digital-land save-state --specification-dir=specification --collection-dir=$(COLLECTION_DIR) --pipeline-dir=$(PIPELINE_DIR) --resource-dir=$(COLLECTION_DIR)resource/ --incremental-loading-override=$(INCREMENTAL_LOADING_OVERRIDE) --output-path=state.json

save-state:: state.json
	aws s3 cp state.json s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/state.json --no-progress

load-state::
	aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)/state.json state.json --no-progress || echo state.json not found in s3 bucket $(COLLECTION_DATASET_BUCKET_NAME)/$(REPOSITORY)
