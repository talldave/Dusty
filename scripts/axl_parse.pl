#!/usr/local/bin/perl -w
use strict;
require "/ferrari1/users/sde/bin/is_data_tools_strict.pl";

&defineArgs("axl","AXL file","MapShop AXL file to be parsed.");
&defineArgs("ws","web_service","Name of web service which AXL file relates to.");
my %opt = &ReadCommandLine('axl:ws');
#$opt{p} = &setOption("p","h_geonet");
#$opt{l} = &setOption("l","AP_RELATION_LING_TEST");
&setOption("log","NONE");


&BeginProgram;
my ($x,$comment,$layerfound,$rv,$n,$symbolpos);
$x = $comment= $layerfound = $rv = $n = $symbolpos = 0;
my ($field,$label,$layerid,$exrg_value,$low,$up,$stag,$mapshop_layer);
$field = $label = $layerid = $exrg_value = $low = $up = $stag = $mapshop_layer = "";
my %axl;

my $string = qq{"([^"\\\\]*(?:\\\\.[^"\\\\]*)*)"}; # matches a string within double-quotes
#my @sym;

#my $logger = &initLogger;

#&sqlConnect("$opt{p}");
#my $sqld = qq{ delete from $opt{l} where mapservice = '$opt{ws}' };
#$rv = &writeQuery($sqld);

open(IN,"$opt{axl}") or &Error("fatal","Couldnt find file: $opt{axl}");
open(OUT,">>axl_parse.txt") or &Error("fatal","Couldnt open file");

while (<IN>) {
  if (/\<\!--/) { $comment = 1; }
  if (/--\>/) { $comment = 0; }
  if ($comment == 0) {
    	if (/\<DATASET /) {
    	    m/name\=$string/;
			print OUT "$1\n";
    	}
    	if (/\<PARTITION /) {
    	    m/name\=$string/;
			print OUT "$1\n";
    	}
  } #end if comment = 0
} #end while(IN)

close(IN);
close(OUT);

&EndProgram;


__END__

