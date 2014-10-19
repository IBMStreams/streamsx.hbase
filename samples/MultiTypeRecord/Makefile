# Copyright (C) 2013-2014, International Business Machines Corporation  
# All Rights Reserved                                	                 

# NOTE: To compile, you must have HBASE_HOME and HBASE_HOME set in your
# environment.

.PHONY: all clean 

# Fill in HBASE_TOOLKIT location here.
STREAMS_HBASE_TOOLKIT ?= ../../com.ibm.streamsx.hbase
SOURCES = com.ibm.streamsx.hbase.sample/MultiTypeGet.spl com.ibm.streamsx.hbase.sample/MultiTypeRecord.spl com.ibm.streamsx.hbase.sample/PutBookRecord.spl com.ibm.streamsx.hbase.sample/ScanTableWithBlob.spl com.ibm.streamsx.hbase.sample/WriteTuples.spl
SPLC_FLAGS ?= --data-directory data -a 
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= -t $(STREAMS_HBASE_TOOLKIT)
SPL_MAIN_COMPOSITE = com.ibm.streamsx.hbase.sample::MultiTypeRecord

all: standalone

data:
	mkdir data

output/bin/standalone: $(SOURCES) data
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) 

output/com.ibm.streamsx.hbase.sample.MultiTypeRecord.adl: $(SOURCES) data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

standalone: output/bin/standalone

distributed: output/com.ibm.streamsx.hbase.sample.MultiTypeRecord.adl

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm data/*.out

