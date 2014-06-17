#!/usr/local/bin/perl
require "is_data_tools_strict.pl";

my %opt = &ReadCommandLine('csv:file');
if (defined $opt{csv}) { &setOption("l","$opt{csv}"); }
&BeginProgram;
my ($server,$inst,$user,$pass) = &getConnectionInfo;


if (defined $opt{file}) { 
	open(IN,"$opt{file}");
	while (<IN>) {
		chop;
		if (! /^\#/) {
			my $csv = "$_";
			&loadCSV("$csv"); 
		}
		
	}
	close(IN);
} else {
	&loadCSV("$opt{csv}"); 
}

&EndProgram;


sub loadCSV {
my $csv = shift;

my $ora_table = "$csv"; $ora_table =~ s/(.csv|.txt|.dat)$//i; 


my $esri_table = "$ora_table"; 
$esri_table =~ s/.(..)$//; $geog = "$1"; $geog =~ tr/[A-Z]/[a-z]/; 
$ora_table = "N_"."$ora_table";



my %mapkey = (
	bg => "13", tr => "12", st => "2", cy => "5", us => "2", zp => "5", dm => "3",
	ma => "4", pl => "7", cb => "5", cs => "10", cd => "4"
);

system "M $csv\n";
open(CSV,"$csv") or die;
$x=0;
while (<CSV>) {
	$x++;
	chomp;
	if ("$x" == 1) { (@line) = split(/\,/); } 
}
&sqlConnect("n_geonettest");

my $sql = qq{ select unique var_name10,type,width,precision from new_bao_metadata 
				where upper(esri_table) = upper('$esri_table') };
my $result_array = &readQueryMultiRow("$sql","n_geonettest");

my $sql_attrs = "";
foreach my $row (@$result_array) {
	my ($var,$type,$width,$precision) = (@$row);
	if ("$precision" != 0) { $width = "$width".",$precision"; }
	$sql_attrs = "$sql_attrs"."$var NUMBER($width),";
}

#if ("$geog" eq "st") { $sql_attrs="$sql_attrs"."ST_ABBREV VARCHAR2(2),"; }  # C2000
#else { $sql_attrs="$sql_attrs"."NAME VARCHAR2(128),"; }




&sqlDisconnect("n_geonettest");

my $ora_index = "$ora_table"; $ora_index =~ s/^n_//i; $ora_index .= "_ix_200704";
#$sql_din = qq{ drop index $ora_table\_name_idx };
#$sql_dii = qq{ drop index $ora_index};

$sql_te = qq{ select table_name from user_tables where table_name = '$ora_table' };
$sql_d = qq{ drop table $ora_table };
$sql_dv = qq{ drop view $ora_table };

$sql_t = qq{ CREATE TABLE $ora_table ( $sql_attrs ID VARCHAR2($mapkey{$geog}) ) };
$sql_g = qq{ grant select on $ora_table to adol };
$sql_i = qq{ create unique index $ora_index on $ora_table (id) tablespace ebis_itab };
$sql_at = qq{ analyze table $ora_table estimate statistics };
$sql_ai = qq{ analyze index $ora_index estimate statistics };


&sqlConnect("$opt{p}");
my @tableexists = &readQuerySingle("$sql_te","$opt{p}");
#if ("$tableexists[0]" eq "N_$ora_table") { return 1; }
if ("$tableexists[0]" eq "N_$ora_table") { &writeQuery("$sql_d","$opt{p}"); }

#&writeQuery("$sql_d","$opt{p}"); 
#&writeQuery("$sql_dv","$opt{p}"); 
&writeQuery("$sql_t","$opt{p}"); 
&writeQuery("$sql_g","$opt{p}");



open(OUT,">$ora_table.ctl") or die;


#OPTIONS (SKIP=1,READSIZE=209715200, BINDSIZE=209715200, ROWS=1000)

print OUT <<"EOF";
OPTIONS (SKIP=1)
LOAD DATA
INFILE '$csv'
BADFILE '$csv.bad'
DISCARDFILE '$csv.dis'
APPEND INTO TABLE $ora_table
(
EOF
		print OUT "  ID CHAR TERMINATED BY ',' ENCLOSED BY '\"',\n";  
		#print OUT "  ID CHAR TERMINATED BY ',' ,\n";  
		#if ("$geog" eq "st")  {
			#print OUT "  ST_ABBREV CHAR TERMINATED BY ',' ENCLOSED BY '\"',\n"; 
		#} else  {
			#print OUT "  NAME CHAR TERMINATED BY ',' ENCLOSED BY '\"',\n"; 
		#}

for ($a=0;$a<$#line;$a++) {
#foreach my $row (@$result_array) {
	#my ($var,$type,$width,$precision) = (@$row);
	$line[$a] =~ s/\"//g;

	if ("$line[$a]" !~ /^(ID|NAME|ST_ABBREV)$/i) {
		print OUT "  $line[$a] DECIMAL EXTERNAL TERMINATED BY ',',\n";
	}

}
	$line[$a] =~ s/\"//g;
		print OUT "  $line[$a] DECIMAL EXTERNAL TERMINATED BY WHITESPACE\n";
		print OUT ")\n";
		print OUT "BEGINDATA\n";


close(OUT);
close(CSV);



&ExecCmd("sqlldr ebis/ebis\@$server control=$ora_table.ctl readsize=1024000 bindsize=1024000 rows=10000");
&writeQuery("$sql_i","$opt{p}");
&writeQuery("$sql_at","$opt{p}");
&writeQuery("$sql_ai","$opt{p}");


}



