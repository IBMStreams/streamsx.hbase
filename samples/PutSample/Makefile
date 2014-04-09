# Copyright (C) 2013-2014, International Business Machines Corporation  
# All Rights Reserved     

# NOTE: To compile, you must have HBASE_HOME and HBASE_HOME set in your
# environment.

.PHONY: all clean 

# Fill in STREAMS_HBASE_TOOLKIT location here.
STREAMS_HBASE_TOOLKIT ?= ../../com.ibm.streamsx.hbase

SPLC_FLAGS = -a 
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= -t $(STREAMS_HBASE_TOOLKIT)
SPL_MAIN_UPDATE = com.ibm.streamsx.hbase.sample::BasicUpdate
SPL_MAIN_CHECK = com.ibm.streamsx.hbase.sample::CheckAndPut
SPL_MAIN_EXIST_CHECK=com.ibm.streamsx.hbase.sample::PutExistCheck
SPL_MAIN_PUT=com.ibm.streamsx.hbase.sample::PutSample


all: standalone_update standalone_check standalone_exist standalone_put

standalone_update:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_UPDATE) $(SPL_CMD_ARGS) --output-dir output_update

standalone_check:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_CHECK) $(SPL_CMD_ARGS) --output-dir output_check

standalone_exist:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_EXIST_CHECK) $(SPL_CMD_ARGS) --output-dir output_check_exist

standalone_put:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_PUT) $(SPL_CMD_ARGS) --output-dir output_put


distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_UPDATE) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_CHECK) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_EXIST_CHECK) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_PUT) $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_UPDATE) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_CHECK) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_EXIST_CHECK) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_PUT) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_UPDATE) $(SPL_CMD_ARGS) --output-dir output_update
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_CHECK) $(SPL_CMD_ARGS) --output-dir output_check
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_EXIST_CHECK) $(SPL_CMD_ARGS) --output-dir output_check_exist
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_PUT) $(SPL_CMD_ARGS) --output-dir output_put
	rm data/*.out

