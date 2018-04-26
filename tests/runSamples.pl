#!/usr/bin/perl
# NOTE: THIS IS A TEMPORARY TEST SCRIPT.  

# It runs only the samples, and assumes all the tables have been created.

use strict;

die "Must have HBASE_HOME specified in environment before running" if  ($ENV{HBASE_HOME} eq "");

die "Must have HADOOP_HOME specified in environment before running" if  ($ENV{HADOOP_HOME} eq "");


my @samples = `ls ../samples`;
my $sampleCount = 0;
my $appCount = 0;
for my $s (@samples) {
    # try the make
    chomp $s;
    system("cd ../samples/$s; make");
    (($? >> 8) == 0) or die "Could not make sample $s";
    $sampleCount++;
    my @apps = `ls ../samples/$s | grep output`;
    
    for my $app (@apps) {
	chomp $app;
	print "Running ../samples/$s/$app/$s/bin/standalone\n";
	system("../samples/$s/$app/$s/bin/standalone");
	(($? >> 8) == 0) or die "Could not not run ../samples/$s/$app/$s/bin/standalone";
	$appCount++;
    }
    
}
print "Found $sampleCount samples, ran $appCount apps\n";
exit 0;
