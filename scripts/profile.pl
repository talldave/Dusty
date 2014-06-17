#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

&defineArgs("o","add | delete | list","Operation");
&defineArgs("u","user","SDE Username");
&defineArgs("pw","password","SDE Password");
&defineArgs("s","server","SDE Server");
&defineArgs("i","instance","SDE Instance");

my %opt = &ReadCommandLine('o:u:pw:s:i:d');
my $database;
&setOption("log","NONE");
&setOption("email","OFF");
&BeginProgram;

&requiredOptions('o');

if (defined $opt{d}) { $database = "$opt{d}"; } else { $database = ""; }

if ("$opt{o}" eq "add") {
	&requiredOptions('p:s:i:u:pw');
	my $sql = qq{ delete from is_users where profile = '$opt{p}' };
	&writeQuery($sql,"sdeadmin");
	
	$sql = qq{ insert into is_users values ('$opt{p}','$opt{s}','$opt{i}','$opt{u}','$opt{pw}','$database','','') };
	&writeQuery($sql,"sdeadmin");
	print "Profile $opt{p} added.\n";
}

if ("$opt{o}" eq "delete") {
	&requiredOptions('p');
	my $sql = qq{ delete from is_users where profile = '$opt{p}' };
	&writeQuery($sql,"sdeadmin");
	print "Profile $opt{p} deleted.\n";
}

if ("$opt{o}" eq "list") {
	my $sql = qq{ select profile,server,instance,username,database from is_users order by profile };
	&print_row($sql,"sdeadmin");
}

&EndProgram;


__END__

