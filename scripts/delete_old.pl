#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

my %opt = &ReadCommandLine;

&setOption("email","NONE");
&setOption("log","OFF");
&BeginProgram;
my ($s,$i,$user,$p) = &getConnectionInfo;
my @master_layerlist;
my %found;
my %a;
my %desc;
my $layer;

foreach my $server ("nutria","eos","iris") {
	if ("$server" eq "nutria") { &setOption("p","n_$user"); }
		elsif ("$server" eq "eos") { &setOption("p","e_$user"); }
		elsif ("$server" eq "iris") { &setOption("p","iris_$user"); }
	&sqlConnect;
	my $sql0 = qq{ select table_name from sde.layers where owner = upper('$user') order by table_name };
	my $resultArray = &readQueryMultiRow($sql0);
	foreach my $row (@$resultArray) {
		($layer) = @$row;
		if (! "$found{$layer}") { push(@master_layerlist,"$layer"); ++$found{"$layer"}; }
		$a{"$server"}{"$layer"}{"count"} = &getCount("$layer");

		%desc = &describeLayer("$layer");
		$a{$server}{$layer}{grid} = "$desc{grid}";
		$a{$server}{$layer}{normal_io} = "$desc{io_mode}";

		my $sql1 = qq{ select grantee from user_tab_privs where table_name = upper('$layer') and grantee = 'ADOL' };
		my @ans1 = &readQuerySingle("$sql1");
		if ("$ans1[0]" =~ /adol/i) {
			$a{$server}{$layer}{adol} = 1;
		} else {
			$a{$server}{$layer}{adol} = 0;
		}

		my $sql2 = qq{ select num_rows from user_tables where table_name = upper('$layer') };
		my $ans2 = &readQuerySingle("$sql2");
		if ("$ans2" ne "") {
			$a{$server}{$layer}{analyzed} = 1;
		} else {
			$a{$server}{$layer}{analyzed} = 0;
		}
	} #end foreach my $row
	&sqlDisconnect;

} #end foreach my $server




my $id=0;
open(OUT,">qc.csv");
foreach my $layer (@master_layerlist) { 
  $id++;
  print OUT "$id,$layer,";
  foreach my $qc ("count","grid","normal_io","adol","analyzed") {
    foreach my $server ("nutria","eos","iris") {
		print OUT "$a{$server}{$layer}{$qc},"; 
	} #end foreach my $server
  } #end foreach my $qc
  print OUT "\n";
} # end foreach my $layer

close(OUT);


&EndProgram;

