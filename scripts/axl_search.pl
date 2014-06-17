#!/usr/local/bin/perl

open(IN,"/ferrari1/users/sde/temp/gdtaxlsearch.in") or die;
while (<IN>) {
	chop;
	my $line = "$_";
	system "echo xxx xxx $line xxx xxx >> /ferrari1/users/sde/temp/gdt_axl_search.txt \n";
	system "grep -il $line  */*axl >> /ferrari1/users/sde/temp/gdt_axl_search.txt \n";
}

close(IN);


