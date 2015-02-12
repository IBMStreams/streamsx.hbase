# Copyright (C) 2013-2014, International Business Machines Corporation  
# All Rights Reserved      

# NOTE: To compile, you must have HBASE_HOME and HBASE_HOME set in your
# environment.

.PHONY: all clean 


TOOLKIT_NAME=com.ibm.streamsx.hbase
# Fill in STREAMS_HBASE_TOOLKIT location here.
STREAMS_HBASE_TOOLKIT ?= $(shell  ([ -e ../../$(TOOLKIT_NAME) ] && echo ../../$(TOOLKIT_NAME)) ||\
	                  ([ -e "../com.ibm.streamsx.hbase" ] && echo ../$(TOOLKIT_NAME)) ||\
	                                           echo $(STREAMS_INSTALL)/toolkits/$(TOOLKIT_NAME))
SPLC_FLAGS ?= -a --data-directory data 
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= -t $(STREAMS_HBASE_TOOLKIT)
SPL_MAIN_COMPOSITE = com.ibm.streamsx.hbase.sample::GetSample

all: standalone

data:
	mkdir data

standalone: data
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

distributed: data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm data/*.out

