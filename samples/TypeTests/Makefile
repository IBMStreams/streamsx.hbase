# Copyright (C) 2013-2014, International Business Machines Corporation  
# All Rights Reserved                                	                 

# NOTE: To compile, you must have HBASE_HOME and HBASE_HOME set in your
# environment.

.PHONY: all clean 

# Fill in HBASE_TOOLKIT location here.
STREAMS_HBASE_TOOLKIT ?= ../../com.ibm.streamsx.hbase
SOURCES = com.ibm.streamsx.hbase.sample/CheckTest.spl com.ibm.streamsx.hbase.sample/DeleteTest.spl com.ibm.streamsx.hbase.sample/PutGetTest.spl com.ibm.streamsx.hbase.sample/TypeTests.spl
SPLC_FLAGS = -a 
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= -t $(STREAMS_HBASE_TOOLKIT)
SPL_MAIN_COMPOSITE = com.ibm.streamsx.hbase.sample::TypeTests

all: standalone

output/bin/standalone: $(SOURCES)
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) 

output/com.ibm.streamsx.hbase.sample.TypeTests.adl: $(SOURCES)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

standalone: output/bin/standalone

distributed: output/com.ibm.streamsx.hbase.sample.TypeTests.adl

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm data/*.out

