include makerules/makerules.mk
include makerules/pipeline.mk


ifeq ($(CONFIG_BUCKET),)
CONFIG_BUCKET=$(ENVIRONMENT)-collection-data/
endif

save-config::
	aws s3 sync collection/ s3://$(CONFIG_BUCKET)config/collection/ 
	aws s3 sync pipeline/ s3://$(CONFIG_BUCKET)config/pipeline/ 