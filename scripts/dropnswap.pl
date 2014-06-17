#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";
#use File::Grep;

&defineArgs("reverse","","Reverse operation");
my %opt = &ReadCommandLine('file:reverse');
&BeginProgram;
&getConnectionInfo;
my $livelayer;

if (defined $opt{file}) { 
	#if ( fgrep {/^n_/i} "$opt{file}") { &Error("fatal","Your layer list must contain only N_ names."); } 
	open(IN,"$opt{file}") or &Error("fatal","Couldnt open $opt{file}");
	while (<IN>) { if ("$_" !~ /^(n_|new_|\#)/i) { &Error("fatal","Your layer list must contain only N_ names."); } } 
	close(IN);
	open(IN,"$opt{file}") or &Error("fatal","Couldnt open $opt{file}");
	while (<IN>) {
		chop;
		my $newlayer = "$_";
		$newlayer =~ s/ //g;
                if ("$newlayer" !~ /^\#/) {
  		  			$newlayer =~ m/^(n_|new_)(.*)/i; 
					$livelayer = "$2";
		  			&dropnswap("$newlayer");
                }
	}
	close(IN);
} else {
	if ("$opt{l}" !~ /^n_/i) { &Error("fatal","Your -l argument must be a NEW_ or N_ layername"); }
    $opt{l} =~ m/^n_(.*)/i; 
	$livelayer = "$1";
	&dropnswap("$opt{l}");
}


sub dropnswap {
	my $newlayer = "@_";
	my $oldlayer = "O_"."$livelayer";
	&setOption("l","$newlayer");
	if (!defined $opt{reverse}) {
		&deleteLayer("$oldlayer"); 
		&renameLayer("$livelayer","$oldlayer");
		&renameLayer("$newlayer","$livelayer");
	} else {
		&renameLayer("$livelayer","$newlayer");
		&renameLayer("$oldlayer","$livelayer");	
	}
	
	&grantADOL("$livelayer");
	&updateLayerDate("$livelayer");
	&describeAsADOL("$livelayer");
}

&EndProgram;
