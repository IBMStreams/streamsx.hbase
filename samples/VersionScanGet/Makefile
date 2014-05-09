# Copyright (C) 2013-2014, International Business Machines Corporation  
# All Rights Reserved                                	                 

# NOTE: To compile, you must have HBASE_HOME and HBASE_HOME set in your
# environment.

.PHONY: all clean 

# Fill in HBASE_TOOLKIT location here.
STREAMS_HBASE_TOOLKIT ?= ../../com.ibm.streamsx.hbase
SRC=$(wildcard com.ibm.streamsx.hbase.sample/*.spl)
SPLC_FLAGS = -a 
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= -t $(STREAMS_HBASE_TOOLKIT)
SPL_MAIN_COMPOSITE_GET_RECORD = com.ibm.streamsx.hbase.sample::MultipleVersions

all: output_getrecord

output_getrecord: $(SRC)
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE_GET_RECORD) $(SPL_CMD_ARGS) --output-dir output_getrecord

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_GET_RECORD) $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE_GET_RECORD)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE_GET_RECORD) --output-dir output_getrecord

