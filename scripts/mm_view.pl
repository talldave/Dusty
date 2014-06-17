#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

#$opt{"proj"}="moneymailer"; 
my $layer="mm_smartzones_v";

&ReadCommandLine;
&setOption("l","MM_SMARTZONES_V");
&BeginProgram;

&getConnectionInfo;

&deleteTable("$layer");
&createView("$layer","mm_zones_layer a,mm_zonecex b,mm_zonemri c,mm_zonedemos d","*","","a.zone=b.zonecex and a.zone=c.zonemri and a.zone=d.zonedemos");
&grantADOL("$layer");

&EndProgram;
