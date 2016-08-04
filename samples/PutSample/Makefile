# Copyright (C) 2013-2016, International Business Machines Corporation  
# All Rights Reserved     

# NOTE: To compile, you must have HBASE_HOME and HBASE_HOME set in your
# environment.

.PHONY: all clean 

TOOLKIT_NAME=com.ibm.streamsx.hbase
STREAMS_HBASE_TOOLKIT ?= $(shell  ([ -e "../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../$(TOOLKIT_NAME)) ||\
			                      ([ -e "../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../$(TOOLKIT_NAME)) ||\
			                       echo $(STREAMS_INSTALL)/toolkits/$(TOOLKIT_NAME))

SPLC_FLAGS ?= -a --data-directory data 
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= -t $(STREAMS_HBASE_TOOLKIT)
SPL_MAIN_UPDATE = com.ibm.streamsx.hbase.sample::BasicUpdate
SPL_MAIN_CHECK = com.ibm.streamsx.hbase.sample::CheckAndPut
SPL_MAIN_EXIST_CHECK=com.ibm.streamsx.hbase.sample::PutExistCheck
SPL_MAIN_PUT=com.ibm.streamsx.hbase.sample::PutSample

SPL_MAIN_COMP= BasicUpdate
SPL_MAIN_CHECK_COMP= CheckAndPut
SPL_MAIN_EXIST_COMP= PutExistCheck
SPL_MAIN_PUT_COMP=PutSample

all: compile data

data:
	mkdir data


compile: data
	$(SPLC) --output-dir output/$(SPL_MAIN_COMP) $(SPLC_FLAGS) -M $(SPL_MAIN_UPDATE) $(SPL_CMD_ARGS)
	$(SPLC) --output-dir output/$(SPL_MAIN_CHECK_COMP) $(SPLC_FLAGS) -M $(SPL_MAIN_CHECK) $(SPL_CMD_ARGS)
	$(SPLC) --output-dir output/$(SPL_MAIN_EXIST_COMP) $(SPLC_FLAGS) -M $(SPL_MAIN_EXIST_CHECK) $(SPL_CMD_ARGS)
	$(SPLC) --output-dir output/$(SPL_MAIN_PUT_COMP)  $(SPLC_FLAGS) -M $(SPL_MAIN_PUT) $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_UPDATE) $(SPL_CMD_ARGS)  --output-dir output/$(SPL_MAIN_COMP)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_CHECK) $(SPL_CMD_ARGS) --output-dir output/$(SPL_MAIN_CHECK_COMP)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_EXIST_CHECK) $(SPL_CMD_ARGS) --output-dir output/$(SPL_MAIN_EXIST_COMP) 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_PUT) $(SPL_CMD_ARGS) --output-dir output/$(SPL_MAIN_PUT_COMP)
	rm data/*.out

