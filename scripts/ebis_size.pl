#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

my %opt = &ReadCommandLine('file');
&setOption("email","OFF");
&setOption("log","NONE");
&BeginProgram;
my ($serv,$inst,$user,$pass) = &getConnectionInfo;
&sqlConnect;

my $total=0;
my $sql0;
my $outfile;
    my @layerArray;


if (defined $opt{file}) {
	open(IN,"$opt{file}") or &Error("fatal","File: $opt{file} does not exist.\n");
    $outfile = "$opt{p}_$opt{file}";
	open(OUT,">dbsize_$outfile.txt");
	while (<IN>) {
		chop;
		&calcSize("$_");
		#push(@layerArray,"$_");
		#$sql0 = qq{ select table_name,layer_id from sde.layers where owner = upper('$user') and table_name = upper('$_')  };
		#my @row = &readQuerySingle($sql0);
		#&calcSize($row[0],$row[1]);
	}
	close(IN);
} else {
	$sql0 = qq{ select table_name,layer_id from sde.layers where owner = upper('$user') order by table_name };
    $outfile = "$opt{p}";
}

print OUT "__LAYER__\t\t__SIZE IN MB__\n";

sub calcSize {
#my $resultArray = &readQueryMultiRow("$sql0");
#foreach my $row (@$resultArray) {
	#my ($layer,$lid) = @$row;
	my $layer = shift;
	my $sql = qq{ SELECT SUM(USER_SEGMENTS.BLOCKS  * 16384 / 1024 / 1024) "TOTAL_SPACE_MB" 
					FROM USER_SEGMENTS 
					WHERE SEGMENT_NAME = upper('$layer')
						OR SEGMENT_NAME IN ( SELECT INDEX_NAME FROM USER_INDEXES WHERE TABLE_NAME = '$layer' ) };
	my @ans2 = &readQuerySingle("$sql");
	my $mb = $ans2[0];
	print OUT "$layer\t\t$mb\n";
	$total += $mb;
#}
} #end sub calcSize

print OUT "\n_TOTAL_\t\t$total MB\n\n";

close(IN);close(OUT);
system "more dbsize_$outfile.txt";
&EndProgram;

