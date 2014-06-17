#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

&defineArgs("x","shp|sdx","Option to export as shapefile or SDE export file.");
my %opt = &ReadCommandLine('file:x');
&requiredOptions('x');
&BeginProgram;

&getConnectionInfo;

if (defined $opt{file}) {
print "file= $opt{file}\n";
        my $layer;
        open(IN,"$opt{file}") or &Error("fatal","Couldnt open $opt{file}");
        while (<IN>) {
        	if ((! /^$/) and (! /^\#/)) {
        	        chop;
        	        $layer = "$_";
        	        &setOption("l","$layer");
        	        &exportLayer("$layer","$opt{x}");
       		}
        }
        close(IN);
} else {
        &exportLayer("$opt{l}","$opt{x}");
}

&EndProgram;

