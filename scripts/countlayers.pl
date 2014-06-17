#!/usr/local/bin/perl
use strict;
require "is_data_tools_strict.pl";

my %opt = &ReadCommandLine;
&BeginProgram;

my ($sql,$profile);

foreach my $server ("nutria","helios","eos") {
#foreach my $server ("helios","eos") {
#foreach my $server ("nutria") {

	$profile = "$server"; $profile =~ /^(.).*$/; $profile = "$1"."_$RO_USER";

	&sqlConnect("$profile");

	my $table = "is_"."$server"."_layers_nv";
	my $x=0;


#retrieve list of ALL layers in is_<server>_layers_nv -or-
	$sql = qq{ select unique owner,object_name from $table };
#retrieve list of layers in is_<server>_layers_nv with featcount=null
	#$sql = qq{ select unique owner,object_name from $table where sde_feature_cnt is null };
	my $result_array = &readQueryMultiRow("$sql","sdeadmin");
	foreach my $row (@$result_array) {
		$x++;
		my $owner = @$row[0]; my $object = @$row[1];

		my $featurecount = &getCount("$owner.$object","$profile");

#loop through list, and get count
#populate _nv table with count

		$sql = qq{ update $table set sde_feature_cnt = ? where owner = ? and object_name = ? };
		&writeQuery("$sql","sdeadmin","$featurecount","$owner","$object");
		if ("$x" == 50) { &sqlCommit("sdeadmin"); $x=0; }
	} #end foreach row

	&sqlCommit("sdeadmin");

} #end foreach server




&EndProgram;



