# Copyright (C) 2013-2014, International Business Machines Corporation  
# All Rights Reserved                                	                 

# NOTE: To compile, you must have HBASE_HOME and optionally HADOOP_HOME set in your
# environment.

.PHONY: all clean 

# Fill in HBASE_TOOLKIT location here.
#STREAMS_HBASE_TOOLKIT ?= ../../../com.ibm.streamsx.hbase
#STREAMS_HBASE_TOOLKIT ?= $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.hbase

OOLKIT_NAME=com.ibm.streamsx.hbase
STREAMS_HBASE_TOOLKIT ?= $(shell  ([ -e "../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../$(TOOLKIT_NAME)) ||\
                 ([ -e "../../../$(TOOLKIT_NAME)" ] && echo ../../../$(TOOLKIT_NAME)) ||\
                 ([ -e "../$(TOOLKIT_NAME)" ] && echo ../$(TOOLKIT_NAME)) ||\
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

