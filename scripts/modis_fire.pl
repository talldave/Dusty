#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";
use LWP::Simple;

my %opt = &ReadCommandLine;
$opt{p} = &setOption("p","n_misc");
$opt{l} = &setOption("l","modis_fire");
&BeginProgram;
&getConnectionInfo;
my $logger = &initLogger;

chdir("$ENV{MODISDATA}");

my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
$yday += 1;
$year += 1900;
if ("$yday" < 100) { $yday = "0"."$yday"; }
my $e00_file = "modis_fire_$year\_$yday.e00";
#my $url = "http://firemapper.sc.egov.usda.gov/modispts/$e00_file.gz";  #changed to new url below 5-27-04
my $url = "http://activefiremaps.fs.fed.us/fireptdata/$e00_file.gz";
$logger->info("GET $url");

#get url.. place in projects/misc dir
#my $HTTP_resp = getstore("$url","$e00_file.gz");
#$logger->info("HTTP response: $HTTP_resp");
#if ("$HTTP_resp" != 200) { &Error("email:fatal","Could not retrieve url: $url"); }
&ExecCmd("wget $url");

#unzip file
&ExecCmd("gunzip $e00_file.gz");

#convert .e00
&ExecCmd("arc \\\&run modis_fire $e00_file");

#truncate_append MODIS_FIRE on nutria
&truncateLayer("$opt{l}");
&ExecCmd("cov2sde -o append -l $opt{l},shape -f modis_fire,point -s nutria -u misc -p misc -a file=modis_fire.att");

#run modis_fire.sql
&ExecCmd("sqlplus misc/misc\@nutria \@modis_fire");

#transfer from nutria to dbserver
&export_import("$opt{p}","iris_misc","init","$opt{l}");
&export_import("$opt{p}","e_misc","init","$opt{l}");

#remove e00 file
$logger->info("Removing $e00_file ....");
unlink("$e00_file"); 

&EndProgram;

__END__
