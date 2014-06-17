#!/usr/local/bin/perl

require "is_data_tools_strict.pl";

&ReadCommandLine('owner');
&setOption("log","NONE");
&setOption("email","OFF");

&BeginProgram;

my ($serv,$inst,$user,$pass) = &getConnectionInfo;

&sqlConnect;

my $sql = qq{ select table_name from sde.layers where owner = upper('$user') };
my $sql2 = qq{ select table_name from sde.raster_columns where owner = upper('$user') };

print "VECTOR:\n";
&print_row("$sql");
print "RASTER:\n";
&print_row("$sql2");


&EndProgram;
