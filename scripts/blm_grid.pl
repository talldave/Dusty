#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

&ReadCommandLine;
&setOption("email","off");
&setOption("log","none");
&BeginProgram;
&getConnectionInfo;
&sqlConnect;
#open(LAYERS,"blm_master.list") or die;
open(LAYERS,"blm_foo.list") or die;
open(OUT,">blm_grid.csv") or die; 
my ($count,$diff,$grid1,$grid2,$sqrt_count);

while(<LAYERS>) {
	chop;
	my $layer="$_";
#my 	$layer = "twp_index";
	my %desc = &describeLayer("$layer");
	$count = &getLayerCount("$layer");
	$diff = $desc{maxx} - $desc{minx};
	$grid1 = $diff / 10; 
	if ($count != 0) { $sqrt_count = sqrt($count); $grid2 = $diff / $sqrt_count; } else { $grid2=0;}

print "$desc{grid}  .. $desc{minx} ... $desc{maxx} .. $diff .. $count .. $sqrt_count ..g1 $grid1 ..g2 $grid2\n";
print OUT "$layer,$desc{grid},$grid2,$count\n";

}
close(LAYERS);
close(OUT);

&EndProgram;
