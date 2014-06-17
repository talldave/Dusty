#!/usr/local/bin/perl -w
use strict;

open(IN,"$ARGV[0]") or die;
open(OUT,">esri.$ARGV[0]") or die;

while (<IN>) {
	if (/^CREATE TABLE (.*) \(/) { print OUT "CREATE TABLE GDT_$1 (\n"; }
	elsif (/^BUS_FID number/) { print OUT "SHAPE number(38),\n"; }
	elsif (/^TABLESPACE /) { print OUT "TABLESPACE GDT_ATAB;\n"; exit;}
	else { print OUT; }

}
close(OUT); close(IN); exit(1);


