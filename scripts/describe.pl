#!/usr/local/bin/perl -w
require "is_data_tools_strict.pl";

my $opt_f_desc = "maxx\n
user_privileges\n
spatial_column\n
layer_type\n
grid2\n
io_mode\n
maxy\n
coordinate_system\n
layer_description\n
entities\n
minimum_shape_id\n
minx\n
table_owner\n
srid\n
grid\n
array_form\n
measure_offset\n
layer_configuration\n
system_units\n
measure_units\n
exist\n
parameter\n
falsex\n
creation_date\n
grid1\n
layer_id\n
miny\n
z_units\n
z_offset\n
table_name\n
falsey\n";
&defineArgs("f","filter","$opt_f_desc");
my %opt = &ReadCommandLine('file:f');
&setOption("log","DUMMY");
&setOption("email","OFF");

&BeginProgram;
&getConnectionInfo;


if (defined $opt{file}) {
	open(IN,"$opt{file}");
	while (<IN>) {
	if (! /^\#/) {  
	chop;
	my $layer = "$_";
	&desc("$layer");
	}
	}
	close(IN);
} else {
	&desc("$opt{l}");
}

sub desc {
my $layer = shift;
my %desc = &describeLayer("$layer");
print "\n\n$layer:\t\t";

if (!defined $opt{f}) { @elemlist = keys %desc; } else { @elemlist = split(/\,/,"$opt{f}"); }

#foreach my $elem ( keys %desc ) {
foreach my $elem ( @elemlist ) {
	print "\n$elem:\t$desc{$elem}\t";
}
print "\n";
}

&EndProgram;


