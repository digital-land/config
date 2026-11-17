# when ran elsewhere only one collection is generally done
# in this repo we can run for any  collection so we force
# the collection to be set and use this to organise where everything is placed
ifeq ($(COLLECTION),)
$(error Environment variable COLLECTION is not set)
endif

# use the local Collection and pipeline directories for that collection
# if there's anything in the bucket rather than here then it will be added
COLLECTION_DIR=collection/$(COLLECTION)/
PIPELINE_DIR=pipeline/$(COLLECTION)/

# for pipeline outputs mmake specific directories so that different ones can be used
# depending the collection your working on
TRANSFORMED_DIR=transformed/$(COLLECTION)/
ISSUE_DIR=issue/$(COLLECTION)/
DATASET_DIR=dataset/$(COLLECTION)/
FLATTENED_DIR=flattened/$(COLLECTION)/
EXPECTATION_DIR = expectations/$(COLLECTION)/

# We are testing migrating to parquet files, as part of this we are creating a new log directory and will 
# be slowly migrating various logs that are created into one directory. dur to name classes the name below is
# used
OUTPUT_LOG_DIR=log/$(COLLECTION)/

# we create a var directorry here to store anything that would be in the var dir
# we strictly don't need this as everything is automatically separated by dataset
VAR_DIR = var/$(COLLECTION)

# use this to make stuff that is normally in var
COLUMN_FIELD_DIR=$(VAR_DIR)/column-field/
DATASET_RESOURCE_DIR=$(VAR_DIR)/dataset-resource/
CACHE_DIR=$(VAR_DIR)/cache/
CONVERTED_RESOURCE_DIR=$(VAR_DIR)/converted-resource/

include makerules/makerules.mk
include makerules/development.mk
include makerules/collection.mk
include makerules/pipeline.mk

ifeq ($(CONFIG_BUCKET),)
CONFIG_BUCKET=$(ENVIRONMENT)-collection-data/
endif

save-config::
	aws s3 sync collection/ s3://$(CONFIG_BUCKET)config/collection/ 
	aws s3 sync pipeline/ s3://$(CONFIG_BUCKET)config/pipeline/ 

# what  to do next
# resource directory is being  

# custom makefile rule for adding endpoints using the add-data command
add-data:
ifeq ($(INPUT_CSV),)
	$(error Provide INPUT_CSV Environment Variable to add data)
	@exit 1
endif
	digital-land add-data $(INPUT_CSV) $(COLLECTION) -c $(COLLECTION_DIR) -p $(PIPELINE_DIR) -o $(CACHE_DIR)organisation.csv

test:: test-unit test-integration test-acceptance

test-unit:
	pytest tests/unit/

test-integration:
	pytest tests/integration/

test-acceptance:
	pytest tests/acceptance/

# Run Manage's checks locally (bin/manage_check.py) if Manage and async are unavailable.
# async-request-backend's code needs Python >= 3.10, so this uses its own venv, not .venv (3.9).
MANAGE_DIR=var/manage/
MANAGE_VENV=$(MANAGE_DIR)venv/
MANAGE_PYTHON_BIN ?= python3.13
# The async-request-backend commit whose Check and Add data code we run. Change it deliberately.
ASYNC_COMMIT=17959d321ab7e10df89e8bef5d942874e3efc6c8
ASYNC_SRC=$(MANAGE_DIR)async-request-backend-$(ASYNC_COMMIT)/request-processor/src/

$(MANAGE_VENV).installed: requirements-manage-check.txt
	$(MANAGE_PYTHON_BIN) -m venv $(MANAGE_VENV)
	$(MANAGE_VENV)bin/pip install --quiet --upgrade pip
	$(MANAGE_VENV)bin/pip install --quiet -r requirements-manage-check.txt
	touch $@

$(ASYNC_SRC)application/core/workflow.py:
	@mkdir -p $(MANAGE_DIR)
	curl -qfsL https://github.com/digital-land/async-request-backend/archive/$(ASYNC_COMMIT).tar.gz | tar -xz -C $(MANAGE_DIR)

# makerules' specification target doesn't download these; refreshed by make init like the rest.
# issue-type.csv gives the Check step its issue severities; provision.csv says who provides each dataset.
specification::
	@mkdir -p specification/
	curl -qfsL '$(SOURCE_URL)specification/main/specification/issue-type.csv' > specification/issue-type.csv
	curl -qfsL '$(SOURCE_URL)specification/main/specification/provision.csv' > specification/provision.csv

.PHONY: manage-setup
manage-setup: $(MANAGE_VENV).installed $(ASYNC_SRC)application/core/workflow.py $(CACHE_DIR)organisation.csv

# make manage-check COLLECTION=local-plan DATASET=plan-timetable ORGANISATION=local-authority:WRL \
#   ENDPOINT_URL='https://...' DOCUMENTATION_URL='https://...'
.PHONY: manage-check
manage-check: manage-setup
	@test -n "$(DATASET)" -a -n "$(ORGANISATION)" -a -n "$(ENDPOINT_URL)" -a -n "$(DOCUMENTATION_URL)" || \
		(echo "Set DATASET, ORGANISATION, ENDPOINT_URL and DOCUMENTATION_URL (and COLLECTION)"; exit 1)
	@test -f specification/dataset.csv -a -f specification/issue-type.csv -a -f specification/provision.csv || \
		(echo "specification/ is missing files - run make init COLLECTION=$(COLLECTION) first"; exit 1)
	PYTHONPATH=$(ASYNC_SRC) $(MANAGE_VENV)bin/python bin/manage_check.py \
		--collection '$(COLLECTION)' --dataset '$(DATASET)' --organisation '$(ORGANISATION)' \
		--endpoint-url '$(ENDPOINT_URL)' --documentation-url '$(DOCUMENTATION_URL)' \
		--organisation-csv $(CACHE_DIR)organisation.csv $(MANAGE_CHECK_FLAGS)
