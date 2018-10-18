#!/usr/bin/perl
# Copyright (C) 2014, 2018, International Business Machines Corporation  
# All Rights Reserved	  
#
# NOTE: To run this script, you must have HBASE_HOME and HBASE_HOME set in your
# environment.
# This script test the Hbase samples.
# It creates tables via Hbase shell in Hbase database.
# Makes the spl application and runs the standalone files.
# It runs the HBASE samples GetRecord  GetSample  PutRecord  PutSample
# in samples directory
# before it begin with test, it creates 2 sample tables on hbase database
# hbase(main):026:0> create 'streamsSample_books', 'all'
# hbase(main):027:0> create 'streamsSample_lotr','appearance','location'
# after the tests it checks the contain of tables with hbase shall
# hbase(main):026:0> scan 'streamsSample_books'
# hbase(main):027:0> scan 'streamsSample_lotr'

use strict;
use Getopt::Long;

die "Must have HBASE_HOME specified in environment before running" if  ($ENV{HBASE_HOME} eq "");

die "Must have HADOOP_HOME specified in environment before running" if  ($ENV{HADOOP_HOME} eq "");

die "Must have STREAMS_INSTALL specified in environment before running" if ($ENV{STREAMS_INSTALL} eq "");

print "\n  $0 runs the the HBASE samples in samples directory\n";

print "\n*****************************************************\n";
print "Drop and create tables on Hbase databse\n";

my $cmd;
my $result;

$cmd = "echo \"list\" | hbase shell";
print "$cmd\n";
$result = `$cmd`;
print "$result \n*****************************************************\n\n";

if (index($result, "HBase") == -1) 
{
	print "Hbase shell cannot connect to the Hbase server\n";
	exit;
} 


$cmd = "echo \"disable 'streamsSample_lotr' ; drop 'streamsSample_lotr'\" | hbase shell";
print "$cmd\n";
$result = `$cmd`;
print "$result \n*****************************************************\n";

$cmd = "echo \"create 'streamsSample_lotr','appearance','location'\" | hbase shell";
print "$cmd\n";
$result = `$cmd`;
print "$result \n*****************************************************\n";

$cmd = "echo \"disable 'streamsSample_books' ; drop 'streamsSample_books'\" | hbase shell";
print "$cmd\n";
$result = `$cmd`;
print "$result \n*****************************************************\n";

$cmd = "echo \"create 'streamsSample_books', 'all'\" | hbase shell";
print "$cmd\n";
$result = `$cmd`;
print "$result \n*****************************************************\n";



$cmd = "echo \"list\" | hbase shell";
print "$cmd\n";
$result = `$cmd`;
print "$result \n*****************************************************\n\n";


my @samples = `ls ../samples | grep -v  README`;
print "Tests in samples directory: \n";
print " @samples\n";

my $sampleCount = 0;
my $appCount = 0;
for my $s (@samples) {
	chomp $s;
	print "\n**************************  Sample $s  *************************\n";
	# try the make
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
print "\n*****************************************************\n";
print "\nFound $sampleCount samples, ran $appCount apps\n\n";
print "*****************************************************\n";

$cmd = "echo \"scan 'streamsSample_lotr'\" | hbase shell";
print "$cmd\n";
$result = `$cmd`;
print "$result \n*****************************************************\n";


$cmd = "echo \"scan 'streamsSample_books'\" | hbase shell";
print "$cmd\n";
$result = `$cmd`;
print "$result \n\n*****************************************************\n";


exit 0;

