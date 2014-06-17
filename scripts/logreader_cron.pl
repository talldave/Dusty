#!/usr/local/bin/perl -w
# logreader_cron.pl -- D.Bianco 11/15/03
use strict;

my ($logstamp,@filter) = split(/\s+/,"@ARGV");
if (!defined $ARGV[0]) { die "USAGE: logreader.pl <logstamp> <filter>\n"; }

#my $filter = "@filter";
#$filter =~ s/\s+/\)\(/g;
#print "filter = $filter\n\n\n";

#my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
#my $logstamp = sprintf("%02d%02d%02d",$year,$mon,$mday);
my $isdt_log = "$ENV{LOGHOME}/cron_$logstamp.log";

my ($found,$a);
open(IN,"$isdt_log") or die "Can't open $isdt_log\n";
while (<IN>) {
	$a=1;$found=0;
	foreach my $elem (@filter) { if (/$elem/) { $found=1;} else {$found=0;$a=0;} }
	if ("$a" == 0) { $found = "$a"; }
	if ("$found" == 1) { print; } 
}
close(IN);

