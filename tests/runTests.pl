#!/usr/bin/perl
use strict;
use Getopt::Long;

die "Must have HBASE_HOME specified in environment before running" if  ($ENV{HBASE_HOME} eq "");

die "Must have HADOOP_HOME specified in environment before running" if  ($ENV{HADOOP_HOME} eq "");

die "Must have STREAMS_INSTALL specified in environment before running" if ($ENV{STREAMS_INSTALL} eq "");


my $numDiff =0;
my $numMakeAndRun =0;
my $numRunCommand = 0;
my $numClearTable = 0;
my $runPattern;
my $disableTableCreation;
my $debug;
GetOptions ("runPattern=s" => \$runPattern,
            "disableTableCreation" =>\$disableTableCreation,
            "debug" => \$debug);
my %commandResults;


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
    system("echo \"disable '$tableName'; drop '$tableName'\; ${createCommand}\" | \$HBASE_HOME/bin/hbase shell > shellResults");
   die "Problems creating table" unless ($? >> 8 == 0) ;
    $debug && print "Table $tableName with columnfamilies $colFam1 $colFam2 is ready.\n";

}

# Probably should copy, then make and run, but not doing that for now.
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

    my $makeString = "cd $dir; make $target";
 
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
	
    $debug && print STDOUT "$runString\n";
    system($runString);
    die "makeAndRun failed" unless ($? >> 8 == 0) ;
}

sub runCommand(%) {
    my %args = @_;
    $numRunCommand++;
    my $command=$args{"command"};
    my $resultKey=$args{"resultKey"};

    my $results = `$command`;
    chomp $results;
    $debug && print "Result of command $command is $results, putting it in under $resultKey\n";
    $commandResults{$resultKey}=$results;
}



sub diff(%) {
        my %args = @_;
	$numDiff++;
	my $expected = $args{"expected"};
	my $actual= $args{"actual"};
	my $replaceTS = exists $args{"replaceTimestamp"};
	$debug && print "Checking $actual\n";
	if ($replaceTS) {
	    $debug && print "sed 's/=14[0-9]*/=TIMESTAMP/g' $actual |  diff $expected -\n";
	    system("sed 's/=14[0-9]*/=TIMESTAMP/g' $actual | diff $expected -");
	}
	else {
	    system("diff $expected $actual");
	}
	die "diff failed: $expected and $actual not the same" unless ($? >> 8 == 0) ;
}


sub tailAndDiff(%) {
        my %args = @_;
	my $expected = $args{"expected"};
	my $actual= $args{"actual"};
	system("tail -n 1 $actual | diff $expected");
	die "tail and diff failed" unless ($? >> 8 == 0) ;
}

sub main() {
my @tests = `ls *.cfg`;

my $numTests =0;
my $numPassed = 0;
for my $testFile (@tests) {
    chomp($testFile);
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
}

print "$numPassed out of $numTests passed\n";
    print "\tDIFF: $numDiff\n";
    print "\tMAKE_AND_RUN: $numMakeAndRun\n";
    print "\tRUN_COMMAND: $numRunCommand\n";
    print "\tCLEAR_TABLE: $numClearTable\n";

}

main();
