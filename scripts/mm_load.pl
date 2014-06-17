#!/usr/local/bin/perl -w
# D.Bianco 06-01-99 ESRI
use strict;
require "is_data_tools_strict.pl";

&ReadCommandLine;
#$opt{"proj"}="moneymailer";
&setOption("p","n_misc");  #$opt{"p"}="n_misc";
&setOption("l","MM_ZONES_LAYER");

my $PROJECT_HOME = "$ENV{MMDATA}";

&BeginProgram;

&getConnectionInfo;

my $layer = "mm_zones_layer";
my $file = "smartzones_demographics.shp";
my $table;


#&deleteLayer("$layer");
    
#&shapeLoadSimple("$file","$layer","-x -180,10,1000000 -g 5.5");
&truncateLayer("$layer");
&shapeAppendSimple("$file","$layer");

&grantADOL("$layer");
&alterReg("$layer");
&updateStats("$layer");
    
    



foreach my $dbf_file (<$PROJECT_HOME/zone*dbf>) {
    
    if ("$dbf_file" =~ /zonecex/) {
        $table = "mm_zonecex";
    } elsif ("$dbf_file" =~ /zonedemos/) {
        $table = "mm_zonedemos";
    } elsif ("$dbf_file" =~ /zonemri/) {
        $table = "mm_zonemri";
    }
    &setOption("l","$table");

    #&deleteLayer("$table");
	&ExecCmd("sdetable -o delete -t $table -s nutria -u misc -p misc -N");
    &createTable("$dbf_file","$table");
    

}


&EndProgram;
