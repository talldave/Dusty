#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

my %opt = &ReadCommandLine('file');

&setOption("email","NONE");
&setOption("log","OFF");
&BeginProgram;
my $count;

	&sqlConnect;
	if (defined $opt{file}) {
		open(IN,"$opt{file}") or die;
		while (<IN>) { 
			if (! /^\#/) {	
				chop; 
				my $layer = "$_";
				$count = &getCount("$layer");
				print "$count\n";
			}
		}
		close(IN);
	} else {
		$count = &getCount("$opt{l}");
		print "$count\n";
	}
	&sqlDisconnect;


&EndProgram;

