#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

my %opt = &ReadCommandLine;
&setOption("email","OFF");
&BeginProgram;
my ($serv,$inst,$user,$pass) = &getConnectionInfo;
&sqlConnect;
my $layer = "$opt{l}";

my $sql1 = qq{ select layer_id from sde.layers where table_name = upper('$layer') and owner = upper('$user') };
my @resp = &readQuerySingle("$sql1");
my $layer_id = "$resp[0]";


my $sql2 = qq{ select eminx,eminy,emaxx,emaxy,fid from f$layer_id };
my $result_array = &readQueryMultiRow("$sql2");

foreach my $row (@$result_array) {
 	my $minx=@$row[0];my $miny=@$row[1];my $maxx=@$row[2];my $maxy=@$row[3];my $fid = @$row[4];

	my $sql3 = qq{ update $layer set minx=$minx,miny=$miny,maxx=$maxx,maxy=$maxy where shape = $fid };
	&writeQuery("$sql3");

}


&sqlDisconnect;
&EndProgram;


__END__


