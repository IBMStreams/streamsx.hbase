# Copyright (C) 2013-2014, International Business Machines Corporation  
# All Rights Reserved                                	                 

# NOTE: To compile, you must have HBASE_HOME and HBASE_HOME set in your
# environment.

.PHONY: all clean 

# Fill in HBASE_TOOLKIT location here.
OOLKIT_NAME=com.ibm.streamsx.hbase
STREAMS_HBASE_TOOLKIT ?= $(shell  ([ -e "../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../$(TOOLKIT_NAME)) ||\
	         ([ -e "../../../$(TOOLKIT_NAME)" ] && echo ../../../$(TOOLKIT_NAME)) ||\
                 ([ -e "../$(TOOLKIT_NAME)" ] && echo ../$(TOOLKIT_NAME)) ||\
	                 echo $(STREAMS_INSTALL)/toolkits/$(TOOLKIT_NAME))

#STREAMS_HBASE_TOOLKIT ?= ../../../com.ibm.streamsx.hbase
#STREAMS_HBASE_TOOLKIT ?= $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.hbase

SRC=$(wildcard com.ibm.streamsx.hbase.sample/*.spl)
SPLC_FLAGS ?= -a --data-directory data 
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= -t $(STREAMS_HBASE_TOOLKIT)
SPL_MAIN_COMPOSITE_GET_RECORD = com.ibm.streamsx.hbase.sample::MultipleVersions

all: distributed

data:
	mkdir data


distributed: data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_GET_RECORD) $(SPL_CMD_ARGS)  --output-dir output/VersionScanGet

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE_GET_RECORD) --output-dir output/VersionScanGet

