#!/usr/local/bin/perl -w
# D.Bianco 06-01-99 ESRI
use strict;
require "is_data_tools_strict.pl";
defineArgs("tl","layer","Target Layer");
defineArgs("dbf","dbf file","Source DBF File");
defineArgs("o","create|append","Option to create or append a DBF file to Oracle table");



my %opt = &ReadCommandLine('pre:dbf:file:o:tl');
&requiredOptions('o');
&BeginProgram;
my ($server,$inst,$user,$pass) = &getConnectionInfo;


if (defined $opt{file}) {
	open(IN,"$opt{file}") or die &Error("fatal","Could not open $opt{file}");
	while (<IN>) {
		if (! /^\#/) {
			chop;
			$opt{dbf}="$_";
			&getConnectionInfo;
			&loadtable;
			if (defined $opt{tl}) { $opt{o} = &setOption("o","append"); }
		}
	}
	close(IN);
} else {
	&loadtable;
}
 
&EndProgram;

sub loadtable {

	my $table = "$opt{tl}" || "$opt{dbf}";
	#&check_attrs("$opt{dbf}");
	$table =~ s/\.dbf//i;
	if (defined $opt{pre}) { $table = "$opt{pre}"."_$table" };

	if ("$opt{o}" eq "create") {
		&deleteTable("$table");
		&createTable($opt{dbf},$table);
	} elsif ("$opt{o}" eq "append") {
		&appendTable($opt{dbf},$table);
	}
	
	&updateStats("$table");
	
	&grantADOL("$table");
	
	#&updateMetadata("$table","tables");
	&getConnectionInfo;

}

sub check_attrs {
	my $shp = shift;
	my $tmpfile = "$shp".".tmp";
	my $attfile = "$shp".".att";
	my $attrs = 0; 
	my $x = 0;
	my $numattrs = 0;
	#system "shpinfo -f $shp > $file\n";
	#open(ATT,"$file") or die &Error("-1","Could not open $file");
	#while (<ATT>) {
	#	if (/[\bdate\b|\bdesc\b|\blong\b|\bgroup\b|\blevel\b|\bsize\b|\bcomment\b|\bnumber\b|\bcount\b|\bcurrent\b|\bmode\b]/) {
	#		print "ERROR: Illegal attribute found\n";
	#	}
	#}
	#close(ATT);
	
	print "shpinfo -f $shp > $tmpfile\n";	
	system "shpinfo -f $shp > $tmpfile\n";
	#system "M $tmpfile\n";
	open(TMP,"$tmpfile") or &Error("-1","Could not open $tmpfile");
	open(ATT,">$attfile") or &Error("-1","Could not open $attfile");
	while (<TMP>) {
	  print;
	  if ( $attrs == 1) {
	    if ($x < $numattrs) {
	      my @line = split(/\s+/,"$_");
	      my $newattr = "$line[0]";
	      if ($newattr =~ /\bdate\b/i) { $newattr = "DATESTAMP"; }
	      if ($newattr =~ /\bdesc\b/i) { $newattr = "DESCRIPTION"; }
	      if ($newattr =~ /\blong\b/i) { $newattr = "LON"; }
	      if ($newattr =~ /\bgroup\b/i) { $newattr = "GRP"; }
	      if ($newattr =~ /\blevel\b/i) { $newattr = "LEVEL_"; }
	      if ($newattr =~ /\bsize\b/i) { $newattr = "SIZE_"; }
	      if ($newattr =~ /\bcomment\b/i) { $newattr = "COMMENTS"; }
	      if ($newattr =~ /\bnumber\b/i) { $newattr = "NUM"; }
	      if ($newattr =~ /\bcount\b/i) { $newattr = "CNT"; }
	      if ($newattr =~ /\bcurrent\b/i) { $newattr = "CRNT"; }
	      if ($newattr =~ /\bmode\b/i) { $newattr = "TA_MODE"; }
	      print ATT "$line[0] $newattr\n";
	    }
	      $x++;
    	  }
    	  if (/^------------ /) { $attrs = 1; }
	  if (/Shape Attribute Columns: (.*)/) { $numattrs = "$1"; }
	}
	close(TMP);close(ATT);
	
}

__END__
