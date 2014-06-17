#!/usr/local/bin/perl 
#################################################################################
#
# Name:		is_dba_metadata.pl
#
# Usage:	.pl 
#
# Description:	Updates metadata tables
# 
#################################################################################
require "is_data_tools.pl";
use DBI;
use File::Basename;

&setup;

&qProfile;

#foreach $server ("NUTRIA","HELIOS","EOS") {
foreach $server ("NUTRIA","HELIOS","EOS") {
  %serv_nv;
  &qMetadata;           # Query Metadata Table 
  &qServer_Layers_V;    # Query Server LAYERS
  &qServer_Raster_V;    # Query Server VIEWS
  &qServer_Views_V;     # Query Server RASTER
  &qServer_Tables_V;    # Query Server TABLES
  &iMetadata;           # Insert into Metadata
  #&qServer_Layers_NV;   # Query Nov-View LAYERS Table
  #&qServer_Raster_NV;   # Query Nov-View RASTER Table
  ##&qServer_Views_NV;   # Update Nov-View VIEWS Table
  ##&qServer_Tables_NV;  # Update Nov-View TABLES Table
  #&uServer_NV;          # Update Nov-View TABLES
  # Add code to delete records from Non-View Tables (CLEANUP)

}
 &uMetadataStatus;
 &uMetadataAXL ;

$dbh->disconnect();

exit;

########################################
sub setup {
  %admin; %helios; %eos; %nutria; %serv_nv; %profile;
  $dbh = DBI->connect('DBI:Oracle:','sdeadmin/sdeadmin@ferrari');
}

########################################
### Get Records from Metadata Table
########################################
sub qMetadata {
  print "Querying Metadata Table...\n";
  undef(%admin);

  my $sql = qq{ SELECT owner, object_name FROM is_dba_metadata};
  my $sth = $dbh->prepare ($sql);
  $sth->execute();

  my ($owner, $object_name);
  $sth->bind_columns( undef, \$owner, \$object_name);

  %admin;
  while( $sth->fetch() ) {
     my $key = "$owner"."."."$object_name";
     $admin{"$key"} = 1;
  }

  $sth-> finish();
}

########################################
sub qServer_Layers_V {
########################################

  my $tab = "IS_"."$server"."_LAYERS_V";
  print "Querying $tab...\n";
  my $sql = qq{ SELECT owner, table_name FROM $tab };
  my $sth = $dbh->prepare($sql);
  $sth->execute();
  my ($owner, $table_name);
  $sth->bind_columns( undef, \$owner, \$table_name);
  while( $sth->fetch() ) { 
    my $lkey = "$owner"."."."$table_name"; 
    ${$server}{$lkey} = "LAYER"; 
    #print "$server:$lkey:${$server}{$lkey}\n";
  }
  $sth-> finish();

} # end sub qServer_Layers_V

########################################
sub qServer_Views_V {
########################################

  my $tab = "IS_"."$server"."_VIEWS_V";
  print "Querying $tab...\n";
  my $sql = qq{ SELECT owner, table_name FROM $tab };
  my $sth = $dbh->prepare($sql);
  $sth->execute();
  my ($owner, $table_name);
  $sth->bind_columns( undef, \$owner, \$table_name);
  while( $sth->fetch() ) { 
    my $vkey = "$owner"."."."$table_name"; 
    ${$server}{$vkey} = "VIEW"; 
  }
  $sth-> finish();

} # end sub qServer_Views_V

########################################
sub qServer_Raster_V {
########################################

  my $tab = "IS_"."$server"."_RASTER_V";
  print "Querying $tab...\n";
  my $sql = qq{ SELECT owner, table_name FROM $tab };
  my $sth = $dbh->prepare($sql);
  $sth->execute();
  my ($owner, $table_name);
  $sth->bind_columns( undef, \$owner, \$table_name);
  while( $sth->fetch() ) { 
    my $rkey = "$owner"."."."$table_name"; 
    ${$server}{$rkey} = "RASTER"; 
  }
  $sth-> finish();

} # end sub qServer_Raster_V

########################################
sub qServer_Tables_V {
########################################

  my $tab = "IS_"."$server"."_TABLES_V";
  print "Querying $tab...\n";
  my $sql = qq{ SELECT owner, table_name FROM $tab };
  my $sth = $dbh->prepare($sql);
  $sth->execute();
  my ($owner, $table_name);
  $sth->bind_columns( undef, \$owner, \$table_name);
  while( $sth->fetch() ) { my $tkey = "$owner"."."."$table_name"; ${$server}{$tkey} = "TABLE"; }
  $sth-> finish();

} # end sub qServer_Tables_V

########################################
sub iMetadata {
########################################

  print "Inserting new records into IS_DBA_METADATA...\n";
  foreach $key (keys %{$server}) {
    if (! $admin{$key} ) {
      $type = "${$server}{$key}";
      print "NewMetadataRecord:$server:$key:$type\n";
      ($owner,$object_name) = split(/\./,$key);    
      my $sql = qq{ insert into IS_DBA_METADATA 
                    (OWNER,OBJECT_NAME,TYPE,SRC_VENDOR,SRC_CONVREQ,SRC_FILETYPE,LOAD_METHOD,DBA_NOTES,STATUS) 
                    values 
                    ('$owner','$object_name','$type','UNK','UNK','UNK','UNK','Automated Metadata Generation','ACTIVE') } ;
      my $sth = $dbh->prepare($sql);
      $sth->execute();
    }
  }

} # end sub iMetadata

########################################
sub qProfile {
########################################

  print "Querying IS_USERS...\n";
  my $sql = qq{ SELECT UPPER(server), instance, UPPER(username), password FROM IS_USERS};
  my $sth = $dbh->prepare ($sql);
  $sth->execute();

  my ($server, $instance, $username, $password);
  $sth->bind_columns( undef, \$server, \$instance, \$username, \$password);

  while( $sth->fetch() ) { 
    $profile{"$server"}{"$username"}{PASS} = "$password"; 
    $profile{"$server"}{"$username"}{INST} = "$instance"; 
  } # end while
    
} # end sub qProfile

########################################
sub qServer_Layers_NV {
########################################
# Sets Hash Variable %serv_nv

 my $tab = "IS_"."$server"."_LAYERS_NV";
 print "Querying $tab...\n";
 my $sql = qq{ SELECT owner, object_name, sde_mode, etype FROM $tab };
 my $sth = $dbh->prepare ($sql);
 $sth->execute();
 my ($owner, $object_name, $sde_mode, $etype);
 $sth->bind_columns( undef, \$owner, \$object_name, \$sde_mode, \$etype);

 while( $sth->fetch() ) { 
   my $lkey = "$owner"."."."$object_name"; 
   $serv_nv{"$server"}{"$lkey"} = "$sde_mode,$etype"; 
 } 
 $sth-> finish();

} # end sub qServer_Layers_NV

########################################
sub qServer_Raster_NV {
########################################

 my $tab = "IS_"."$server"."_RASTER_NV";
 print "Querying $tab...\n";
 my $sql = qq{ SELECT owner, object_name, raster_id FROM $tab };
 my $sth = $dbh->prepare ($sql);
 $sth->execute();

 my ($owner, $object_name, $layer_id);
 $sth->bind_columns( undef, \$owner, \$object_name, \$layer_id);

 while( $sth->fetch() ) { 
   my $rkey = "$owner"."."."$object_name"; 
   $serv_nv{"$server"}{"$rkey"}{"$layer_id"} = "1"; 
 }
 $sth-> finish();

} # end sub qServer_Raster_NV

########################################
sub pTest {
########################################

 # TEST
 while (($key,$value) = each(%{$server})) {
   print "$server, $key, $value\n";
 }

} # end sub pTest

########################################
sub uServer_NV {
########################################
 my ($own,$obj,$pass);
 print "Updating IS Server Non View tables ...\n";

 foreach $vkey (sort keys %{$server}) {
   ($own,$obj) = split(/\./,$vkey);    
   $pass = "$profile{$server}{$own}{PASS}";
   #print "VKEY--$vkey, SERV--$server, OWN--$own, OBJ--$obj, PASS--$pass\n"; 
   if (${$server}{$vkey} =~ /LAYER/) {
     &uServer_Layers_NV($server,$own,$obj,$pass);
   } elsif (${$server}{$vkey} =~ /RASTER/) {
     &uServer_Raster_NV($server,$own,$obj,$pass);
   } elsif (${$server}{$vkey} =~ /VIEWS/) {
     ##print  "sdelayer -o describe -l $obj,shape -o $own -p $pass -s $server > $tmpfile\n";
   } elsif (${$server}{$vkey} =~ /TABLE/) {
     ##print  "sdetable -o describe -t $obj -o $own -p $pass -s $server > $tmpfile\n";
   } #end if

 } # end foreach 

 $tab = "IS_"."$server"."_LAYERS_NV";
 $vtab = "IS_"."$server"."_LAYERS_V";
 my $sql = qq{ delete from $tab where OWNER||OBJECT_NAME NOT IN 
               (select OWNER||TABLE_NAME from $vtab) } ;
 my $sth = $dbh->prepare($sql);
 $sth->execute();

 $tab = "IS_"."$server"."_RASTER_NV";
 $vtab = "IS_"."$server"."_RASTER_V";
 my $sql = qq{ delete from $tab where OWNER||OBJECT_NAME NOT IN 
               (select OWNER||TABLE_NAME from $vtab) } ;
 my $sth = $dbh->prepare($sql);
 $sth->execute();
  
} # end Sub uServer_NV

########################################
sub uServer_Layers_NV {
########################################
  my($srv,$usr,$obj,$pwd) = @_;
  my $tmpfile = "/ferrari1/users/sde/desc.tmp";
  my ($att,$val);
  my $key = "$usr"."."."$obj"; 
  #print  "sdelayer -o describe_long -l $obj,shape -u $usr -p $pwd -s $srv > $tmpfile\n";
  system "sdelayer -o describe_long -l $obj,shape -u $usr -p $pwd -s $srv > $tmpfile\n";

  open(TMPFILE,"$tmpfile") or die;
 
  while (<TMPFILE>) {
    ($att,$val) = split(/\:/,$_);    
    if ($att =~ /Entities/)  { $val =~ s/ //g; chop $val; $type = "$val"; }
    if ($att =~ /I\/O Mode/) { $val =~ s/ //g; chop $val; $mode = "$val"; }
  } # end while TMPFILE
  close(TMPFILE);
  unlink($tmpfile);
    
  # INSERT / UPDATE 
  $tab = "IS_"."$srv"."_LAYERS_NV";
  if (! $serv_nv{"$srv"}{"$key"} ) {
      print "Inserting into $tab, $usr, $obj, $mode, $type\n";
      my $sql = qq{ insert into $tab (OWNER,OBJECT_NAME,SDE_MODE,ETYPE) values 
                    ('$usr','$obj','$mode','$type') } ;
      my $sth = $dbh->prepare($sql);
      $sth->execute();
  } else {
      ($otype,$omode) = split(/\./,$serv_nv{"$srv"}{"$key"});    
      if ((! $omode == $mode) || (! $otype == $type)) {
        print "Updating $tab, $usr, $obj, $omode->$mode, $otype->$type\n";
        my $sql = qq{ update $tab set SDE_MODE = '$mode', ETYPE = '$type'
                      where OBJECT_NAME = '$obj' and OWNER = '$usr' } ;
        my $sth = $dbh->prepare($sql);
        $sth->execute();
      } else { print "Nothing to update for $usr.$obj in $tab... \n"; }
  }
  
} # end sub uServer_Layers_NV

########################################
sub uServer_Raster_NV {
########################################
  my($srv,$usr,$obj,$pwd) = @_;
  my $tmpfile = "/ferrari1/users/sde/desc.tmp";
  my ($sql); my %raster;
  my ($att,$val);
  my $key = "$usr"."."."$obj"; 

  system  "sderaster -o list -V -l $obj,image -u $usr -p $pwd -s $srv > $tmpfile\n";
  open(TMPFILE,"$tmpfile") or die;
  #$x = 0; 
  while (<TMPFILE>) {
    ($att,$val) = split(/\:/,$_);    

    if ($att =~ /Raster ID/) { $val =~ s/ //g; chop $val; $rid  = "$val"; }
    
    if ($att =~ /Raster Dimension/)      { $val =~ s/ //g; chop $val; $raster{"$rid"}{DIM} = "$val"; }
    if ($att =~ /Raster Tile Dimension/) { $val =~ s/ //g; chop $val; $raster{"$rid"}{TILEDIM} = "$val"; }
    if ($att =~ /Pixel Type/)            { $val =~ s/ //g; chop $val; $raster{"$rid"}{PIXEL_TYPE} = "$val"; }
    if ($att =~ /Compression/)           { $val =~ s/ //g; chop $val; $raster{"$rid"}{COMPRESSION} = "$val"; }
    if ($att =~ /Image Pyramid/)         { $val =~ s/ //g; chop $val; $raster{"$rid"}{PYRAMID} = "$val"; }
    if ($att =~ /minx/)                  { $val =~ s/ //g; chop $val; $raster{"$rid"}{MINX} = "$val"; }
    if ($att =~ /miny/)                  { $val =~ s/ //g; chop $val; $raster{"$rid"}{MINY} = "$val"; }
    if ($att =~ /maxx/)                  { $val =~ s/ //g; chop $val; $raster{"$rid"}{MAXX} = "$val"; }
    if ($att =~ /maxy/)                  { $val =~ s/ //g; chop $val; $raster{"$rid"}{MAXY} = "$val"; }
    if ($att =~ /Colormap/)              { $val =~ s/ //g; chop $val; $raster{"$rid"}{COLORMAP} = "$val"; }
    if ($att =~ /Statistics/)            { $val =~ s/ //g; chop $val; $raster{"$rid"}{STATISTICS} = "$val"; }
    if ($att =~ /min /)                  { $val =~ s/ //g; chop $val; $raster{"$rid"}{STAT_MIN} = "$val"; }
    if ($att =~ /max /)                  { $val =~ s/ //g; chop $val; $raster{"$rid"}{STAT_MAX} = "$val"; }
    if ($att =~ /meam /)                 { $val =~ s/ //g; chop $val; $raster{"$rid"}{STAT_MEAN} = "$val"; }
    if ($att =~ /std dev/)               { $val =~ s/ //g; chop $val; $raster{"$rid"}{STAT_STDDEV} = "$val"; }
   
  } # end while TMPFILE
  
  close(TMPFILE);
  unlink($tmpfile);

  foreach $key (keys %raster) {

    $dim         = "$raster{$key}{DIM}"; 
    $tiledim     = "$raster{$key}{TILEDIM}";
    $compression = "$raster{$key}{COMPRESSION}";
    $pyramid     = "$raster{$key}{PYRAMID}";
    $pixel_type  = "$raster{$key}{PIXEL_TYPE}"; 
    $statistics  = "$raster{$key}{STATISTICS}"; 

    $minx = "$raster{$key}{MINX}"; $miny = "$raster{$key}{MINY}";
    $maxx = "$raster{$key}{MAXX}"; $maxy = "$raster{$key}{MAXY}";

    if (! $raster{$key}{COLORMAP} ) { $colormap = "NO_COLORMAP";             }
    else                            { $colormap = "$raster{$key}{COLORMAP}"; }

    if (! $raster{$key}{STATISTICS} =~ /<NONE>/ ) { 
        $stats;
        $statistics  = "$raster{$key}{STATISTICS}"; 
        $stat_min    = "$raster{$key}{STAT_MIN}";  $stat_max    = "$raster{$key}{STAT_MAX}"; 
        $stat_mean   = "$raster{$key}{STAT_MEAN}"; $stat_stddev = "$raster{$key}{STAT_STDDEV}"; 
    }

    $stat_list = "OWNER,OBJECT_NAME,RASTER_ID,RASTER_DIMENSION,RASTER_TILE_DIMENSION,PIXEL_TYPE,COMPRESSION,IMAGE_PYRAMID,MINX,MINY,MAXX,MAXY,COLORMAP,STATISTICS,STAT_MIN,STAT_MAX,STAT_MEAN,STAT_STDDEV";
    $nostat_list = "OWNER,OBJECT_NAME,RASTER_ID,RASTER_DIMENSION,RASTER_TILE_DIMENSION,PIXEL_TYPE,COMPRESSION,IMAGE_PYRAMID,MINX,MINY,MAXX,MAXY,COLORMAP,STATISTICS";

    $list = 
    # INSERT / UPDATE 
    $tab = "IS_"."$srv"."_RASTER_NV";
    # Add Handle No Colormap
    # Add Handle Blank Statistics
    print "$srv:$usr.$obj:$key\n";
    if (! $serv_nv{"$srv"}{"$usr.$obj"}{"$key"} ) {
        print "Inserting into $tab for $usr.$obj RasterID=$key\n";
        if ($stats) {
          $sql = qq{ insert into $tab ($stat_list) values
                     ('$usr','$obj',$key,'$dim','$tiledim','$pixel_type',
                     '$compression','$pyramid',$minx,$miny,$maxx,$maxy,'$colormap',
                     '$statistics',$stat_min,$stat_max,$stat_mean,$stat_stddev) } ;
        } else {
          $sql = qq{ insert into $tab ($nostat_list) values 
                     ('$usr','$obj',$key,'$dim','$tiledim','$pixel_type',
                     '$compression','$pyramid',$minx,$miny,$maxx,$maxy,'$colormap',
                     '$statistics') } ;
        }
        my $sth = $dbh->prepare($sql);
        $sth->execute();
    } else {
        print "Updating $tab for $usr.$obj RasterID=$key\n";
        if ($stats) {
          $sql = qq{ update $tab set 
                     RASTER_DIMENSION = '$dim', RASTER_TILE_DIMENSION = '$tiledim',
                     PIXEL_TYPE = '$pixel_type', COMPRESSION = '$compression', IMAGE_PYRAMID = '$pyramid', 
                     MINX = $minx, MINY = $miny, MAXX = $maxx, MAXY = $maxy,
                     COLORMAP = '$colormap', STATISTICS = '$statistics', 
                     STAT_MIN = $stat_min, STAT_MAX = $stat_max, STAT_MEAN = stat_mean, STAT_STDDEV = $stat_stddev
                     where OBJECT_NAME = '$obj' and OWNER = '$usr' and RASTER_ID = '$key'} ;
        } else {
          $sql = qq{ update $tab set 
                     RASTER_DIMENSION = '$dim', RASTER_TILE_DIMENSION = '$tiledim',
                     PIXEL_TYPE = '$pixel_type', COMPRESSION = '$compression', IMAGE_PYRAMID = '$pyramid', 
                     MINX = $minx, MINY = $miny, MAXX = $maxx, MAXY = $maxy,
                     COLORMAP = '$colormap', STATISTICS = '$statistics'
                     where OBJECT_NAME = '$obj' and OWNER = '$usr' and RASTER_ID = '$key'} ;
          my $sth = $dbh->prepare($sql);
          $sth->execute();
        }
    } # end if
  } # end foreach

} # end sub uServer_Raster_NV

########################################
sub uServer_Table_NV {
########################################
   
} # end sub uServer_Table_NV


########################################
sub uMetadataStatus {
########################################
  print "Updating IS_DBA_METADATA - STATUS\n";

  my $sql = qq{ update IS_DBA_METADATA SET STATUS = 'ACTIVE' };
  my $sth = $dbh->prepare($sql);
  $sth->execute();

  foreach $key (keys %admin) {
    if ((! "$NUTRIA{$key}") && (! "$HELIOS{$key}") && (! "$EOS{$key}")) {
      print "Updating_IS_DBA_METADATA:$key:INACTIVE\n";
      ($owner,$object_name) = split(/\./,$key);    
      my $sql = qq{ update IS_DBA_METADATA SET STATUS = 'INACTIVE' where OBJECT_NAME = '$object_name' and OWNER = '$owner'};
      my $sth = $dbh->prepare($sql);
      $sth->execute();
    } # end if
  } # end foreach

} # end sub uMetadataStatus

########################################
sub uMetadataAXL {
#########################################
  use File::Basename;
  foreach $key (keys %admin) {
    my ($own,$obj) = split(/\./,$key);    
    my $tmpfile = "/ferrari1/users/sde/desc.tmp";
    print "Updating IS_DBA_METADATA -- AXL\n";
    my ($val,$axl,$cnt,$axl_all);
    #print  "grep -c -i $key /ferrari3/vss/shadow/utilityscripts/ArcIMSAdminLoader/helios/*/* | grep -v \":0\"> $tmpfile\n";
    system "grep -c -i $key /ferrari3/vss/shadow/utilityscripts/ArcIMSAdminLoader/helios/*/* | grep -v \":0\"> $tmpfile\n";

    open(TMPFILE,"$tmpfile") or die;
    while (<TMPFILE>) {
     $val = basename($_);
      ($axlt,$cnt) = split(/\:/,$val);    
      ($axl,$junk) = split(/\./,$axlt);    
      $axl =~ tr/[a-z]/[A-Z]/;
      if (! $axl_all) { $axl_all = "$axl" }
      else            { $axl_all = "$axl".", "."$axl_all"} 
    } # end while TMPFILE
    close(TMPFILE);
    unlink($tmpfile);

    $tab = "IS_DBA_METADATA";
    my $sql = qq{ update $tab set AXL = '$axl_all' where OBJECT_NAME = '$obj' and OWNER = '$own' } ;
    print "$sql\n";
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    undef($axl_all);undef($val);undef($axl);undef($cnt);
  } # end foreach

} # end sub uMetadataAXL

