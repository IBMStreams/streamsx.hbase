##################################################################### 
# Copyright (C)2015, International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################

.PHONY: all clean

#Change to Path to bluemix, bytes and inet toolkits on your machine.
HBASE_BLUEMIX ?= ../../com.ibm.streamsx.hbase.bluemix 
BYTES_TOOLKIT ?= ../../../streamsx.bytes/com.ibm.streamsx.bytes 
INET_TOOLKIT ?= ../../com.ibm.streamsx.inet

SPLC_FLAGS = -a -t $(HBASE_BLUEMIX) -t $(BYTES_TOOLKIT) -t $(INET_TOOLKIT)

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE = TestHBase
STANDALONE_OUTPUT_DIR = output/Standalone
DISTRIBUTED_OUTPUT_DIR = output/Distributed


all: distributed

standalone:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) 

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean: clean-standalone clean-distributed

clean-standalone:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE)
	-rm -rf toolkit.xml

clean-distributed:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE)
	-rm -rf toolkit.xml
