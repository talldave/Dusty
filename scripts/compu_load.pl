#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

my %opt = &ReadCommandLine('file');
&setOption("p","n_gdt");
&BeginProgram;
&getConnectionInfo;
&sqlConnect;
my $dircount=0;
my $rv;
my $logger = &initLogger;
my $path = "$ENV{COMPUDATA}";
my %layers; my @provinces;
my $layer;

#### Check for all Canadian Provinces/ setup stuff
&setup;

if (defined $opt{file}) {
  open (IN,"$opt{file}") or &Error("FATAL","Could not open $opt{file}");
  while (<IN>) {
        chop;
        $layer = "$_"; $layer =~ tr/[a-z]/[A-Z]/;
        &setOption("l","$layer");
        $logger->info("Working on $layer");
        if ("$layer" !~ /^\#/) {
                my $lfile = "$path"."/"."$layer".".log";
                unlink ($lfile);
                open (OUT,">$lfile") or &Error("FATAL","Could not open $lfile");
		&load;
        	$logger->info("Back from load");
                close(OUT);
        }
        $logger->info("Out of loop");
        $dircount=0;
  }
  close(IN);
} else {
        $layer = "$opt{l}"; $layer =~ tr/[a-z]/[A-Z]/;
        my $lfile = "$path"."/"."$layer".".log";
        open (OUT,">$lfile") or &Error("FATAL","Could not open $lfile");
        &load;
        close(OUT);
}

&EndProgram;

###############################################
sub setup {
###############################################
  my ($rv,$sql,$dir);

  $rv = &ExecCmd("chmod -R 777 $path/*");
  $rv = &ExecCmd("chmod 777 $path/*/*");
  #$rv = &ExecCmd("rename \'tr/[A-Z]/[a-z]/\' $path/*");
 
  $sql = qq{ select lower(abbr) from adol.ca_regions order by abbr };
  my $resultArray = &readQueryMultiRow($sql);
  foreach my $row (@$resultArray) {
      	my($prov) = @$row;
      	push(@provinces,$prov);

	#### Set everything to lowercase
      	#$rv = &ExecCmd("rename \'tr/[A-Z]/[a-z]/\' $path/$prov/*");
	
	#### Check for existing directories
      	$dir = "$path"."/"."$prov"  ;
      	if (! -d $dir) { &Error("FATAL","GDT Canada directory $prov does not exist"); }
      	else { $logger->info("Canada Province Directory:: $prov :: EXISTS"); }
  }

  $sql = qq{ select lower(loadfilter),tgt_table_name from tgt_master 
		where project = 'GDT_Canada' order by loadfilter };
  my $result_array = &readQueryMultiRow($sql,"sdeadmin");
  foreach my $row (@$result_array) {
    my($lfilter,$table) = @$row;
    $layers{"$table"} = "$lfilter";
  }

} # end sub 

###############################################
sub load {
###############################################
# Dave recommends listing all of the directories here, instead of "*"
# @provinces = ("ab", "bc", "mb", "nb", "nf", "ns", "nt", "nu", "on", "pe", "qc", "sk", "yt");

  #$logger->info("Loading $layer..."); 
  foreach my $prov (@provinces) {  
        $logger->info("Loading province $prov for $layer..."); 
        my $dir = "$path"."/"."$prov";
	chdir("$dir");
	foreach my $shpfile (<$prov*$layers{$layer}*.shp>) {	
		$dircount++;
                my $shp = basename($shpfile);
		my $shpcount = &shapeInfo("$shpfile");
		print OUT "$layer\t$shp\t$shpcount\n";		
		#### do something with shpcount
		if ("$dircount" == 1) { &truncateLayer("$layer"); }
		&shapeAppendSimple("$shpfile","$layer");
        } # foreach $shpfile
  } # foreach $prov
  $logger->info("Done loading $layer..."); 

} # end sub

