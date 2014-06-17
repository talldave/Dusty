#!/usr/local/bin/perl -w
###################################################################
#
# Name:    noaa.pl
# Usage:   noaa.pl
# Purpose: This script is used to automate the process for PMEL download of raster imagery from
#          NOAA's website, processing in ArcINFO, and finally uploading to production.
#          This script is intended to run as a cron job, downloading and updating nightly.
# 
# History: Original Code- Laurie Fitzpatrick Aug/Sept 2003
#          Modifications- David Bianco Jan 14,2004
###################################################################

use strict;
require "is_data_tools_strict.pl";

&ReadCommandLine;
&setOption("l","NOAA");
&BeginProgram;
my $logger = &initLogger;

chdir("$ENV{PROJHOME}/noaa");

my ($wk,$mm,$dd,$ltime,$yr) = split(/\s+/,localtime());
my $day = "$dd"."$mm"."$yr";
my $FTP_SITE = "ftp.pmel.noaa.gov";
my $FTP_USER = "anonymous";                # NOAA ftp user account name
my $FTP_PASS = "lfitzpatrick\@esri.com";    # NOAA ftp user password
my $ftp_dir = "OCRD/dai/raster"; 
my $keeperpath = "/ferrari5/sde/projects/noaa";
my $raspath    = "$keeperpath"."/ftp";      # FTP Path
my $gridpath   = "$keeperpath"."/grids";    # GRID Path
my $tifpath    = "$keeperpath"."/tifs";     # TIF Path
my $clrpath    = "$keeperpath"."/colormap"; # COLORMAP Path
my $rclpath    = "$keeperpath"."/reclass";  # RECLASS Path
my $amlpath    = "$ENV{VSS}";

my %pmel;
$pmel{airt}   = "airt_xy_hf.ras";
$pmel{dynht}  = "dynht_xy_hf.ras";
$pmel{heat}   = "heat_xy_hf.ras";
$pmel{iso20}  = "iso20_xy_hf.ras";
$pmel{rh}     = "rh_xy_hf.ras";
$pmel{sst}    = "sst_xy_hf.ras";
$pmel{tbar}   = "tbar_xy_hf.ras";
$pmel{uwnd}   = "uwnd_xy_hf.ras";
$pmel{vwnd}   = "vwnd_xy_hf.ras";
$pmel{wspd}   = "wspd_xy_hf.ras";

my @rasfile = sort values %pmel;

## Download RAS files from FTP site 
&ftpConnect("$FTP_SITE","$FTP_USER","$FTP_PASS");
&ftpChangeDir("$ftp_dir");
foreach my $file (@rasfile) {
	my $retrys = 0;my $rv;
	do {
		$rv = &ftpGet("$file","$raspath/$file");
		$retrys++;
	} until ($rv || ($retrys > 10));
	# do ftpGet until successful (max 10 times)
}
&ftpDisconnect;


## Convert RAS files to ArcINFO GRID, process in ArcINFO, then convert to .tif 
&noaa_arc;

## Load .tif files into existing ArcSDE Raster 
&noaa_sde;

&EndProgram;



#########################
sub noaa_arc {
#########################
 	$logger->info("Converting from ASCII to GRID");
 	foreach my $layer (sort keys %pmel) {
     		my $ras   = "$raspath"."/"."$pmel{$layer}";
     		my $tif   = "$tifpath"."/"."$layer".".tif";
     		my $tfw   = "$tifpath"."/"."$layer".".tfw";
     		my $clr   = "$clrpath"."/noaa_"."$layer".".clr";
     		my $rcl   = "$rclpath"."/noaa_"."$layer".".rcl";
	
     		if (-e $tif) { unlink($tif); }
     		if (-e $tfw) { unlink($tfw); }
  	
	        my $aml = "$amlpath"."/noaa.aml";
	     	if ((-e $clr) && (-e $rcl) && (-e $ras)) {
	     		&ExecCmd("arc \\\&r $aml $layer");
     		} else {
		     	$logger->error("ERROR- File missing for $layer...");
	     		$logger->error("\tColormap... $clr");
	     		$logger->error("\tRAS... $ras");
	     		$logger->error("\tReclass Table... $rcl");
       			&Error("EMAIL:FATAL","Missing files :: noaa_arc");
     		} #end if clr & rcl & ras
     		if ((! -e $tif) || (! -e $tfw)) {
	      		&Error("EMAIL:FATAL","RAS:GRID:Tif Conversion Unsuccessful for $layer :: noaa_arc");
	     	} #end if tif or tfw
 	} #end foreach layer
} # end noaa_arc

#########################
sub noaa_sde {
#########################
	foreach my $key (sort keys %pmel) {
        	$logger->info("Loading $key into ArcSDE");
		my $tif   = "$tifpath"."/"."$key".".tif";
		my $tfw   = "$tifpath"."/"."$key".".tfw";
		my $sql;
		my $layer = "NOAA_"."$key";
		my $n = "$day";	
		my $i = "esri_sde"; 
                my @im;
		my $l = "-1"; my $in = "nearest"; my $c = "lz77"; my $g = "4326"; 

		if ( -e $tif ) { 
			foreach my $profile ("n_misc_raster","h_misc_raster","e_misc_raster") {
			#foreach my $profile ("n_misc_raster") {
				my ($server,$inst,$user,$pass) = &getConnectionInfo("$profile");
				&sqlConnect("$profile");

			        $sql = qq { select image from $layer };
                                @im = &readQuerySingle("$sql","$profile");

				#my $cmd = "sderaster -o insert -l $layer,image -s $server -u $user -p $pass -f $tif -n $n -i $i -L $l -I $in -G $g";
				my $cmd = "sderaster -o update -l $layer,image -s $server -u $user -p $pass -f $tif -n $n -i $i -L $l -I $in -G $g -v @im";
				my $rv = &ExecCmd("$cmd");

			        $sql = qq { select image from $layer where name = '$n' };
                                @im = &readQuerySingle("$sql","$profile");
				my $statcmd = "sderaster -o stats -l $layer,image -s $server -u $user -p $pass -i $i -v @im";
				&ExecCmd("$statcmd");

				#if ("$rv" == 0) {
				#	$sql = qq{ update $layer set flag = flag + 1 where flag is not null };
				#	&writeQuery("$sql","$profile");

				#	$sql = qq{ update $layer set flag = 1 where flag is null };
				#	&writeQuery("$sql","$profile");

				#} else { 
				#	&Error("EMAIL:FATAL","Did not load $key properly"); 
				#}  #end if rv=0


				##NOAA_SDECLEAN##
				#$sql = qq{ select image,flag from $layer where flag > 1 order by flag};
				#my $result_array = &readQueryMultiRow("$sql","$profile");

				#foreach my $row (@$result_array) {
				#	my $image = @$row[0]; my $flag = @$row[1];
				#	$logger->info("Image $image is $flag days old...");
				#	my $rv = &deleteMosaic("$layer","$image");
				#        #my $dcmd = "sderaster -o delete -y -l $layer,image -s $server -u $user -p $pass -v $image";
				#        #my $rv = &ExecCmd("$dcmd");
  
			        #	if ("$rv" !=0) { &Error("EMAIL:FATAL","Could not delete $image from $layer"); }
				#} #end foreach row
				&sqlDisconnect("$profile");
			} # end foreach profile
		} else { 
			&Error("EMAIL:FATAL","Processing $key.  $tif does not exist :: noaa_sde"); 
		} #end if -e tiff
	} # end foreach key

###  Add check in SQL for each raster for today's date. 
###  Add update statitics

} # end NOAA_SDE

__END__
