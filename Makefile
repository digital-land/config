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

test:: test-unit test-integration

test-unit:
	pytest tests/unit/

test-integration:
	pytest tests/integration/

test-acceptance:
	pytest tests/acceptance/
