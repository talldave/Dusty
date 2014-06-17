#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

#$opt{"proj"} = "moneymailer";
#$opt{"p"} = "n_misc";
my $PROJECT_HOME = "$ENV{MMDATA}";

my %opt = &ReadCommandLine;
$opt{p} = &setOption("p","n_misc");
$opt{l} = &setOption("l","MM_ZONES_LAYER");

&BeginProgram;

my ($server,$inst,$user,$pass) = &getConnectionInfo;
my $logger = &initLogger;

&ExecCmd("sqlplus $user/$pass\@$server \@mm_mail");

my %found;

++$found{"004-00F"};

my ($dates,$sql,$dbf_file,$zone,$city,$state,$dma,$circ);

my $dbf_handle = &dbfConnect("$PROJECT_HOME");

&sqlConnect("$opt{p}");

foreach $dbf_file ("mailed.dbf","nonmailed.dbf") {

	$logger->info("*** $dbf_file ***");

	if ("$dbf_file" eq "nonmailed.dbf") {
		$sql = qq{ select zone,city,state,dma,circ from $dbf_file };
	} elsif ("$dbf_file" eq "mailed.dbf") {
		$sql = qq{ select zone,city,state,dma,circ,date from $dbf_file };
	}

	$dates = "";
	#print "\n$sql\n";
	my $sth = $dbf_handle->prepare($sql) or die("\n\ncouldnt prepare: \n$dbf_handle->errstr\n\n$sql\n");
	
	$sth->execute or die("\n\ncouldnt execute: \n$sth->errstr\n\n$sql\n");

	while (my @row = $sth->fetchrow_array ) { 
		if (! $found{"$row[0]"}) {
			my $sqlu = qq{ update MM_ZONES_LAYER set circ=?,mail_date=?,city=?,state=?,dma=? where zone = ? };
			&writeQuery("$sqlu","$opt{p}","$circ","$dates","$city","$state","$dma","$zone");
			$dates = ""; ++$found{"$row[0]"};
		}
		$zone = "$row[0]";$city = "$row[1]";$state = "$row[2]";
		$dma = "$row[3]";$circ = "$row[4]";

		if ("$dbf_file" eq "mailed.dbf") {
			$row[5] =~ m/(\d{4})(\d{2})(\d{2})/;
			$row[5] = "$2"."/"."$3"."/"."$1";
			$dates = "$row[5] "."$dates";
		}  #end if mailed

	} ## end while
}  ## end foreach $dbf_file

&EndProgram;
