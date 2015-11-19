##################################################################### 
# Copyright (C)2015, International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################

.PHONY: all clean

#This assumes all the toolkits have been unpacked into a common "toolkits" folder.
#Change to Path to bluemix, bytes and inet toolkits on your machine.
HBASE_BLUEMIX ?= ../../toolkits/com.ibm.streamsx.hbase.bluemix 
BYTES_TOOLKIT ?= ../../toolkits/com.ibm.streamsx.bytes 
INET_TOOLKIT ?= ../../toolkits/com.ibm.streamsx.inet

SPLC_FLAGS = -a -t $(HBASE_BLUEMIX) -t $(BYTES_TOOLKIT) -t $(INET_TOOLKIT)

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE_MAKE_PUT = HBaseMakeTableAndPut
SPL_MAIN_COMPOSITE_GET = HBaseGet
SPL_MAIN_COMPOSITE_DELETE = HBaseDeleteTable
STANDALONE_OUTPUT_DIR = output/Standalone
DISTRIBUTED_OUTPUT_DIR = output/Distributed


all: distributed

standalone:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE_MAKE_PUT) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_MAKE_PUT) $(SPL_CMD_ARGS) 
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE_GET) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_GET) $(SPL_CMD_ARGS) 
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE_DELETE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_DELETE) $(SPL_CMD_ARGS) 

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_MAKE_PUT) --data-directory /tmp --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_MAKE_PUT) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_GET) --data-directory /tmp --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_GET) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_DELETE) --data-directory /tmp --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_DELETE) $(SPL_CMD_ARGS)

clean: clean-standalone clean-distributed

clean-standalone:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE_MAKE_PUT) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_MAKE_PUT)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE_GET) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_GET)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE_DELETE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_DELETE)
	-rm -rf toolkit.xml

clean-distributed:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE_MAKE_PUT) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_MAKE_PUT)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE_GET) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_GET)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE_DELETE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE_DELETE)
	-rm -rf toolkit.xml
