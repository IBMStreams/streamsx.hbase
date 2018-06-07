# Copyright (C) 2014, 2018, International Business Machines Corporation  
# All Rights Reserved                                	                 

# NOTE: To compile, you must have HBASE_HOME and HBASE_HOME set in your
# environment.

.PHONY: all clean 

# Fill in HBASE_TOOLKIT location here.
TOOLKIT_NAME=com.ibm.streamsx.hbase
STREAMS_HBASE_TOOLKIT ?= $(shell ([ -e "../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../$(TOOLKIT_NAME)) ||\
	                         ([ -e "../../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../../$(TOOLKIT_NAME)) ||\
	                         ([ -e "../../../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../../../$(TOOLKIT_NAME)) ||\
                                 ([ -e "../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../$(TOOLKIT_NAME)) ||\
	                          echo $(STREAMS_INSTALL)/toolkits/$(TOOLKIT_NAME))

SOURCES = com.ibm.streamsx.hbase.sample/MultiTypeGet.spl com.ibm.streamsx.hbase.sample/MultiTypeRecord.spl com.ibm.streamsx.hbase.sample/PutBookRecord.spl com.ibm.streamsx.hbase.sample/ScanTableWithBlob.spl com.ibm.streamsx.hbase.sample/WriteTuples.spl
SPLC_FLAGS ?= --data-directory data -a 
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= -t $(STREAMS_HBASE_TOOLKIT)
SPL_MAIN_COMPOSITE = com.ibm.streamsx.hbase.sample::MultiTypeRecord

all: distributed

data:
	mkdir data


distributed: $(SOURCES) data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm data/*.out

