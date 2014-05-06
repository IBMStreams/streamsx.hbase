#!/usr/bin/perl
use strict;
use Getopt::Long;

die "Must have HBASE_HOME specified in environment before running" if  ($ENV{HBASE_HOME} eq "");

die "Must have HADOOP_HOME specified in environment before running" if  ($ENV{HADOOP_HOME} eq "");

die "Must have STREAMS_INSTALL specified in environment before running" if ($ENV{STREAMS_INSTALL} eq "");


my $runPattern;
GetOptions ("runPattern=s" => \$runPattern);


sub clearTable(%) {
    my %args = @_;
    my $tableName = $args{"tableName"};
    my $colFam1 = $args{"firstColumnFamily"};
    my $colFam2 = $args{"secondColumnFamily"};

    my $createCommand = "create '$tableName', '$colFam1'";
    if (defined $colFam2 && $colFam2 =~ /\w/) {
	$createCommand .= ", \'$colFam2\'";
    }
    system("echo \"disable '$tableName'; drop '$tableName'\; ${createCommand}\" | \$HBASE_HOME/bin/hbase shell > shellResults");
   die "Problems creating table" unless ($? >> 8 == 0) ;
    print "Table $tableName with columnfamilies $colFam1 $colFam2 is ready.\n";

}

# Probably should copy, then make and run, but not doing that for now.
sub makeAndRun(%) {
    my %args = @_;
    my $dir = $args{"dir"};
    my $target= $args{"target"};
    my $prog = "output/bin/standalone";
    if (exists $args{"exec"}) {
	$prog = $args{"exec"}."/bin/standalone";
    }
    print STDOUT "cd $dir; make $target; $prog\n";
    system("cd $dir; make $target; $prog");
    die "makeAndRun failed" unless ($? >> 8 == 0) ;
}

sub diff(%) {
        my %args = @_;
	my $expected = $args{"expected"};
	my $actual= $args{"actual"};
	print "Checking $actual\n";
	system("diff $expected $actual");
	die "diff failed" unless ($? >> 8 == 0) ;
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

for my $testFile (@tests) {
    chomp($testFile);
    next unless (!$runPattern || $testFile =~/$runPattern/);
    open(INFILE,"<$testFile"); 

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
    print "Test $testFile passed!\n";
}
}

main();
