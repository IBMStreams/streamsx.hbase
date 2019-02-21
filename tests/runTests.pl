#!/usr/bin/perl
# Copyright (C) 2014, 2018, International Business Machines Corporation  
# All Rights Reserved	  
#
# NOTE: To run this script, you must have HBASE_HOME and HBASE_HOME set in your
# environment.
# This script check the test files (*.cfg).
# Creates tables via Hbase shell in Hbase database.
# Makes the spl application and runs the standalone files.
# At the end, it compares the produced files via spl applications and expected files.
#


use strict;
use Getopt::Long;

die "Must have HBASE_HOME specified in environment before running" if  ($ENV{HBASE_HOME} eq "");

die "Must have HADOOP_HOME specified in environment before running" if  ($ENV{HADOOP_HOME} eq "");

die "Must have STREAMS_INSTALL specified in environment before running" if ($ENV{STREAMS_INSTALL} eq "");




my $numDiffs       = 0;
my $succeededDiffs = 0;
my $failedDiffs    = 0;
my $numMakeAndRun  = 0;
my $numRunCommand  = 0;
my $numClearTable  = 0;
my $runPattern;
my $disableTableCreation;
my $debug;

GetOptions ("runPattern=s" => \$runPattern,
			"disableTableCreation" =>\$disableTableCreation,
			"debug" => \$debug);

my %commandResults;


sub dropTable(%) {

	my %args = @_;
	my $tableName = $args{"tableName"};

	print "\ndrop table $tableName \n";
    my $cmd = "echo \"disable '$tableName'\; drop '$tableName'\" | \$HBASE_HOME/bin/hbase shell > shellResults";
    print "$cmd\n";
	system("echo \"disable '$tableName'; drop '$tableName'\" | \$HBASE_HOME/bin/hbase shell > shellResults");
	die "Problems droping table" unless ($? >> 8 == 0) ;
	print ("Table $tableName droped.\n\n");

}


sub clearTable(%) {
	if ($disableTableCreation) {
		return;
	}

	$numClearTable++;
	my %args = @_;
	my $tableName = $args{"tableName"};
	my $colFam1 = $args{"firstColumnFamily"};
	my $colFam2 = $args{"secondColumnFamily"};
	my $colFam3 = $args{"thirdColumnFamily"};
	my $colFam4 = $args{"fourthColumnFamily"};

	print "\ncreate table $tableName \n";

	my $createCommand = "create '$tableName', '$colFam1'";
	if (defined $colFam2 && $colFam2 =~ /\w/) {
		$createCommand .= ", \'$colFam2\'";
	}
	if (defined $colFam3 && $colFam3 =~ /\w/) {
		$createCommand .= ", \'$colFam3\'";
	}
	if (defined $colFam4 && $colFam4 =~ /\w/) {
		$createCommand .= ", \'$colFam4\'";
	}
	print ("$createCommand\n");
	system("echo \"disable '$tableName'; drop '$tableName'\; ${createCommand}\" | \$HBASE_HOME/bin/hbase shell > shellResults");
	die "Problems creating table" unless ($? >> 8 == 0) ;
	print ("Table $tableName with columnfamilies $colFam1 $colFam2 is ready.\n\n");

}

# Make the spl application via Makefile in the tests directoty and run the standalone file
sub makeAndRun(%) {
	my %args = @_;
	$numMakeAndRun++;
	my $dir = $args{"dir"};
	my $target= $args{"target"};
	my $prog = "output/bin/standalone";
	if (exists $args{"exec"}) {
		$prog = $args{"exec"}."/bin/standalone";
	}


	# first make.
	print ("\n***************** Sample: $dir *********************\n");
	my $makeString = "cd $dir; make $target";
	print "\ncommand:  $makeString\n\n";
	system($makeString);
	die "make failed" unless ($? >> 8 == 0) ;

	my $runString = "cd $dir; $prog";

	for (my $i = 0; $i < 10; $i++) {
		my $thisParam="param".$i;
		my $thisValue = "value".$i;
		if ($args{$thisParam}) {
			my $param = $args{$thisParam};
			$debug && print "Print Popupating $thisParam which is $param\n";
			my $value;
			if ($args{$thisValue}) {
				$value = $args{"value".$i};
			}
			elsif ($args{$thisValue."_key"}) {
				$value = $commandResults{$args{$thisValue."_key"}};
			}
			else {
				die "Cannot populate value for parameter $i, name $param\n";
			}
			$runString .= " ${param}=${value}";
		}
	}

	# run the application
	print ("\ncommand: $runString\n\n");
	system($runString);
	die "makeAndRun failed" unless ($? >> 8 == 0) ;
}


sub runCommand(%) {
	my %args = @_;
	$numRunCommand++;
	my $command=$args{"command"};
	my $resultKey=$args{"resultKey"};

	print ("\ncommand: $command \n");
	my $results = `$command`;
	chomp $results;
	print ("Result of command $command is $results, putting it in under $resultKey\n");
	$commandResults{$resultKey}=$results;
}



sub diff(%) {
	my %args = @_;
	$numDiffs++;
	my $expected = $args{"expected"};
	my $actual= $args{"actual"};
	my $replaceTS = exists $args{"replaceTimestamp"};
	my $cmd="";
	my $result="";
	$debug && print "Checking $actual\n";
	if ($replaceTS) {
		$debug && print "sed 's/[0-9]\\\{13\\\}/TIMESTAMP/p' $actual |  diff $expected -\n";
		$cmd="sed 's/[0-9]\\\{13\\\}/TIMESTAMP/p' $actual | diff $expected -";
	}
	else {
		$cmd="diff $expected $actual";
	}
	print "$cmd\n";	
	$result=`$cmd`;

	if (length( $result ) > 1)
	{
		$failedDiffs++;
		print "ERROR   diff failed: $expected and $actual not the same\n";
	}
	else {
		$succeededDiffs++;
		print "diff succeeded: $expected and $actual are the same\n";
	}

}


sub tailAndDiff(%) {
	my %args = @_;
	my $expected = $args{"expected"};
	my $actual= $args{"actual"};
	system("tail -n 1 $actual | diff $expected");
	die "tail and diff failed" unless ($? >> 8 == 0) ;
}


sub main() {

	print ("\n$0 check the test files (*.cfg)\nCreates tables via Hbase shell in Hbase database.\n");
	print ("It makes the spl application and runs the standalone files.\n"); 
	print ("At the end, it compares the produced files via spl applications and expected files.\n\n");
	print ("Check if Hbase shell is available.\n"); 
	my $cmd = "echo 'list' | hbase shell";
	print "$cmd\n";
	my $result = `$cmd`;

	if (index($result, "HBase") == -1) 
	{
			print "Hbase shell cannot connect to the Hbase server\n";
			exit;
	} 


	$cmd = " echo 'list' | hbase shell | grep '^streamsSample'";
	print "$cmd\n";
	$result = `$cmd`;
#	print "$result\n";
	
	my @tables = split('\n', $result);
	
	
	foreach my $table (@tables) {
	    print "drop Table $table\n";
	    my %currentArgs; 
	    $currentArgs{"tableName"} = $table;
	    dropTable(%currentArgs);
	    sleep(10);
	 }
	 
	$cmd = " echo 'list' | hbase shell";
	print "$cmd\n";
	$result = `$cmd`;
	print "$result\n";
	

	print ("\n*****************************  test files  *****************************\n");
	my @tests = `ls *.cfg`;
	for my $testFile (@tests) {
		chomp($testFile);
		print ("$testFile \n");
	}

	my $numTests =0;
	my $numPassed = 0;
	for my $testFile (@tests) {
		chomp($testFile);
		print ("\n*****************************  $testFile  *****************************\n");
		next unless (!$runPattern || $testFile =~/$runPattern/);
		open(INFILE,"<$testFile"); 
		$numTests++;
		my %currentArgs; 
		my $command;
		while(<INFILE>) {
			next if /^\#/;
			chomp;
			if (/^$/) {
				next unless defined $command;
				next unless $command =~ /\w/;
				if ($command eq "CLEAR_TABLE") {
					clearTable(%currentArgs);
				}
				elsif ($command eq "MAKE_AND_RUN") {
					makeAndRun(%currentArgs);
				}
				elsif ($command eq "COMMAND") {
					runCommand(%currentArgs);
				}
				elsif ($command eq "DIFF") {
					diff(%currentArgs);
	
				}
				elsif ($command eq "TAIL_AND_DIFF") {
					tailAndDiff(%currentArgs);
				}
				else {
					die "Unknown command $command";
				}

				%currentArgs={};
				$command="";
			}
			elsif (/^([A-Z_]+)/) {
				$command = $1;
			}
			elsif (/^([A-Za-z0-9_]+)=(.*)/) {
				$currentArgs{$1} = $2;
				# print "read arg $1 as $2\n";
			}
		}
		close INFILE;
		$numPassed++;
		print "Test $testFile passed!\n";

		print ("*****************************  results  *****************************\n");

		print "\t$numPassed from : $numTests passed\n";
		print "\tnumTests        : $numTests\n";
		print "\tnumTests        : $numPassed\n";
		print "\tDIFFs           : $numDiffs\n";
		print "\tSucceeded DIFFs : $succeededDiffs\n";
		print "\tfailed DIFFs    : $failedDiffs\n";
		print "\tMAKE_AND_RUN    : $numMakeAndRun\n";
		print "\tRUN_COMMAND     : $numRunCommand\n";
		print "\tCLEAR_TABLE     : $numClearTable\n";

	}
}

main();


