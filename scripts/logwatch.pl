#!/usr/local/bin/perl -w
#logwatch.pl -- D.Bianco 11/14/03
use strict;
use File::Tail;

my $pid = shift @ARGV;
if (!defined $pid) { die "USAGE:  logwatch.pl <pid>\n"; }

my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
$year -= 100;
$mon++;
	
my $logstamp = sprintf("%02d%02d%02d",$year,$mon,$mday);
my $isdt_log = "$ENV{LOGHOME}/isdt_$logstamp.log";

my $file = new File::Tail( name => $isdt_log, tail => -1 );
	
print "logfile: $isdt_log\n\n";

while ($_ = $file->read) {  if (/\]\[\:$pid\:\]\[/) { print; }  }

__END__