#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

my %opt = &ReadCommandLine('file');
&setOption("email","off");
&setOption("log","none");
&BeginProgram;
&getConnectionInfo;
&sqlConnect;
#open(LAYERS,"blm_master.list") or die;
open(OUT,">gridsize.txt") or die; 
my ($count,$diff,$grid1,$grid2,$sqrt_count);

if (!defined $opt{file}){
	my $layer="$opt{l}";
	my %desc = &describeLayer("$layer");
	$count = &getLayerCount("$layer");
	$diff = $desc{maxx} - $desc{minx};
	$grid1 = $diff / 10; 
	if ($count != 0) { $sqrt_count = sqrt($count); $grid2 = $diff / $sqrt_count; } else { $grid2=0;}

print "$desc{grid}  .. $desc{minx} ... $desc{maxx} .. $diff .. $count .. $sqrt_count ..g1 $grid1 ..g2 $grid2\n";
print "$layer,$desc{grid},$grid2,$count\n";

} else {
open(LAYERS,"$opt{file}") or die;
while(<LAYERS>) {
	chop;
	my $layer="$_";
	my %desc = &describeLayer("$layer");
	$count = &getLayerCount("$layer");
	$diff = $desc{maxx} - $desc{minx};
	$grid1 = $diff / 10; 
	if ($count != 0) { $sqrt_count = sqrt($count); $grid2 = $diff / $sqrt_count; } else { $grid2=0;}

print "$desc{grid}  .. $desc{minx} ... $desc{maxx} .. $diff .. $count .. $sqrt_count ..g1 $grid1 ..g2 $grid2\n";
print OUT "$layer,$desc{grid},$grid2,$count\n";

}
}
close(LAYERS);
close(OUT);

&EndProgram;
