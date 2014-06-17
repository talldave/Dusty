#!/usr/local/bin/perl 
#################################################################################
#
# Name:		compu_inventory.pl
#
# Usage:	compu_inventory.pl <input_directory> 
#
# Description:	This script performs an inventory for GDT Canada (Compu) data.
#               It finds all shapefiles in the input directory/<valid_province_abbrev>
#               (@provinces), determines the shape count, 
#               inserts a record into the SRC_INVENTORY for each inventoried shape file
#               and updates IS_DBA_METADATA to report the new total shapefile counts
#               for a given layer.
# 
# Requires the following database object:
#               SRC_INVENTORY
#               TGT_MASTER
#               IS_DBA_METADATA
#
# History:	Laurie Fitzpatrick 08/2003
#
# Notes:	Need to implement the following:  
#		- Update/Insert of SRC_PROVIDER_ID, currently inserts null
#		- Better implementation of 'already inventoried' check
#               - Requires the TGT_MASTER be kept up-to-date for the appropriate Layer Names/Abbreviations 
#               - The year is hardcoded. Currently - 2003
#               - Requires the table_name from TGT_MASTER be found in IS_DBA_METADATA 
#                 to update the feature counts.
# 
#################################################################################
require "is_data_tools_strict.pl";
use File::Basename;
use DBI;
$indir = "$ARGV[0]";
if (! $indir)    { die "\nUSAGE: compu_inventory.pl <data_directory>\n";}
if (! -d $indir) { die "Directory $indir does not exist, EXITING...\n"; }

# Input -d Input_directory
$opt{"proj"} = "GDT_Canada";
$proj = "GDT_Canada";
&ReadCommandLine;
&BeginProgram;

$tmpfile = "foo.tmp";
($sec,$min,$hour,$mday,$mon,$year) = localtime();

my %compu; 
my %canada;
my @provinces = ("ab","bc","mb","nb","nl","ns","nt","nu","on","pe","qc","sk","yt");

############################################################
# Check to see if the inventory has already been done.
############################################################
$sql = qq{ select * from src_inventory where LOCATION = '$indir' and project = '$proj'};
undef(@ans);
@ans = &readQuerySingle($sql,"sdeadmin");
print "<@ans>";
#if (@ans) { die "Inventory for $indir already performed...\n"; }

############################################################
# Loop through all provinces in the $indir
############################################################
foreach my $province (@provinces) {
  $prov = "$indir"."/"."$province";
  #print "\nProcessing $prov...\n";
  #print "$prov/*.shp\n";
  
  #########################
  # Loop through all shapefiles for province 
  #########################
  foreach my $file (<$prov/*.shp>) {
    #$file  =~ m/(\D{2})(\d{2})(\D{2}).*/      ;
    $files = basename($file);
    #print "FILES-- $files, FILE-- $file\n";
    $files =~ m/(\D{2})(\d{2})(\D{2}).*/      ;
    my $pref = "$1"; my $suff = "$3" ;
    $suff =~  tr/[a-z]/[A-Z]/;
    #print "PREF-- $pref, SUFF-- $suff...\n";
  
    #########################
    # Get the correct Layer Name from TGT_MASTER
    #########################
    $sql = qq{ select TGT_TABLE_NAME from TGT_MASTER where loadfilter = '$suff' and project = '$proj'};
    undef(@ans);
    @ans = &readQuerySingle($sql,"sdeadmin");
    $table_name = "@ans";

    #########################
    # Get Shapefile Counts
    #########################
    system "shpinfo -o describe -f $file > $tmpfile\n";

    undef($cnt); undef($sum);

    open(TMPFILE,"$tmpfile") or die;

    while (<TMPFILE>) {
      if (/Number of Shapes:\s+(.*)/) {
        $stsum = 0;
        $cnt = "$1"; $cnt =~ s/ //g; chop $cnt;
        $provsum = $cnt + $compu{"$table_name"}; 
        $canada->{$pref}->{$table_name} = $cnt; 
        #print      "$file $cnt...\n";
        $compu{"$table_name"} = $provsum;
      } # end if
    }# end while 
     # ADD CODE TO UPDATE LOG FILES
     #DATA_NAME DATA_TYPE CNT LOCATION TGT_TABLE_NAME RASTER_ID
     $sqlu = qq{INSERT into SRC_INVENTORY values ('$files','SHP',$cnt,'$indir','$table_name',null,'$proj',to_date('$mday-$mon-2003','DD-MM-YYYY'),null) };
     #print "$sqlu\n";
     &writeQuery($sqlu,"sdeadmin");

    $statement = "$file".":"."$table_name".":"."$cnt".":"."$provsum";
    close(TMPFILE);
    unlink("$tmpfile");
  
  } # end for

} # foreach while

############################################################
# Get GDT CANADA ArcSDE Table Names from TGT_MASTER
############################################################
my @compu_tables;
$dbh_ = DBI->connect('DBI:Oracle:','sdeadmin/sdeadmin@ferrari');
my $sql = qq { SELECT  tgt_table_name from tgt_master where project = 'GDT_Canada' order by tgt_table_name};
my $sth = $dbh_->prepare($sql);
$sth->execute();
my ($tgt_table_name);
$sth->bind_columns(undef, \$tgt_table_name);
while($sth->fetch() ){
  push(@compu_tables,"$tgt_table_name"); 
}
$sth->finish();

############################################################
# Print List of GDT Canada Shape Counts by ArcSDE Table Name
############################################################
foreach my $key1 (sort keys %$canada) {
  foreach $table (@compu_tables) {
    if (! $canada->{$key1}->{$table}) {
      print "0\t--$key1:$table\n";
    }
    else {
      my $val = $canada->{$key1}->{$table};
      print "$val\t$key1:$table\n"; 
    }
  }
} 

############################################################
# Print Totals for each Layer and Update the IS_DBA_METADATA Table
############################################################
foreach my $key (sort keys %compu) {
  my $value = $compu{$key};
  print "Total for $key: $value\n"; 

  # Update IS_DBA_METADATA
  $sqlu = qq{UPDATE IS_DBA_METADATA set SRC_FEATURE_CNT = $value,  MDATE = to_date('$mday-$mon-2003','DD-MM-YYYY') where owner = 'GDT' and object_name = '$key'};
  print "$sqlu\n";
  #&writeQuery($sqlu,"sdeadmin");
} 

close(TLOG);

#&EndProgram;

