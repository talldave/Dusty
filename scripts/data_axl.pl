#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

&ReadCommandLine;
&BeginProgram;

open(IN,"helios_all_layers.lst");
my $tmpfile="/ferrari1/users/sde/temp/heliosdata.tmp";
chdir("/ferrari3/vss/shadow/utilityscripts/ArcIMSAdminLoader/helios");
my @axl_list;
&sqlConnect("sdeadmin");

while (<IN>) {
	undef(@axl_list);
	chomp;
	my $count=0;
	my $layer = "$_";
	system "grep -l $layer */*.axl > $tmpfile\n";
	open(IN2,"$tmpfile");
	while (<IN2>) {
		$count++;
		chomp;
		push(@axl_list,"$_");
	}
	close(IN2);
	unlink("$tmpfile");
	my $sql = qq{ insert into is_data_axl values ('$layer','@axl_list',$count) };
	&writeQuery("$sql","sdeadmin");
}
close(IN);


&EndProgram;

