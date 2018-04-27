#!/usr/bin/perl
# It runs the HBASE samples GetRecord  GetSample  PutRecord  PutSample
# in samples directory
# before you begin with you have to create 2 sample tables on hbase
# login in hbase (hdp or iop) server
# $> su - hbsae
# $> hbase shell
# hbase(main):026:0> create 'streamsSample_books', 'all'
# hbase(main):027:0> create 'streamsSample_lotr','appearance','location'
# after tests you can check the contain of tables with hbase shall
# hbase(main):026:0> scan 'streamsSample_books'
# hbase(main):027:0> scan 'streamsSample_lotr'

use strict;
use Getopt::Long;

die "Must have HBASE_HOME specified in environment before running" if  ($ENV{HBASE_HOME} eq "");

die "Must have HADOOP_HOME specified in environment before running" if  ($ENV{HADOOP_HOME} eq "");

die "Must have STREAMS_INSTALL specified in environment before running" if ($ENV{STREAMS_INSTALL} eq "");


my @samples = `ls ../samples | grep -v  README`;
print "Tests in samples directory: \n";
print "@samples\n";

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


