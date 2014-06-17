#!/usr/local/bin/perl
require "is_data_tools_strict.pl";

my %opt = &ReadCommandLine('shp');
&setOption("log","NONE");
&setOption("email","OFF");
my $atttmpfile = "$opt{shp}.att.tmp";
my $attfile = "$opt{shp}.att";
&BeginProgram;

&ExecCmd("shpinfo -f $opt{shp}","$atttmpfile");


open(IN,"$atttmpfile");
open(OUT,">$attfile");
$f=0;

while (<IN>) {
	chop;	
	if (/^------/) { $f=0; }

	if ("$f" == 1) {
		($att,$typ,$wid,$dec) = split(/\,/);
		print OUT "$att $att\n";
	}

	if (/^------/) { $f=1; }

}
close(IN);
close(OUT);
unlink("$atttmpfile");

&EndProgram;

