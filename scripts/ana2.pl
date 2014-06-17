#!/usr/local/bin/perl
require "is_data_tools_strict.pl";

my %opt = &ReadCommandLine("file");
&BeginProgram;
&getConnectionInfo;
&sqlConnect;
print "begin\n";

#&sqlConnect("n_system");
my $sql_t = qq{ select owner,table_name from dba_tables where num_rows is null and owner in (select unique owner from sde.layers)};
my $result_array = &readQueryMultiRow("$sql_t");

foreach my $row (@$result_array) {              
	my $owner = @$row[0]; my $table = @$row[1];
	$owner =~ tr/[a-z]/[A-Z]/;
	$table =~ tr/[a-z]/[A-Z]/;
	#my $dosql = qq{ analyze table $owner.$table estimate statistics };
	my $dosql = qq{ dbms_stats.gather_table_stats(ownname=>'$owner',tabname=>'$table',partname=>NULL,estimate_percent=>10) };
	print "$dosql;\n";
	#&writeQuery("$dosql");
}

my $sql_i = qq{ select owner,index_name from dba_indexes where num_rows is null and owner in (select unique owner from sde.layers)};
my $result_array = &readQueryMultiRow("$sql_i");

foreach my $row (@$result_array) {              
	my $owner = @$row[0]; my $index = @$row[1];
	$owner =~ tr/[a-z]/[A-Z]/;
	$index =~ tr/[a-z]/[A-Z]/;
	#my $dosql = qq{ analyze index $owner.$index estimate statistics };
my $dosql = qq{ dbms_stats.gather_index_stats(ownname=>'$owner',indname=>'$index',partname=>NULL,estimate_percent=>10) };
	print "$dosql;\n";
	#&writeQuery("$dosql");
}

print "end;\n";

&EndProgram;
