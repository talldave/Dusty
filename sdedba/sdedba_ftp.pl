#!/usr/local/bin/perl -w
use strict;
#
### David Bianco  Nov. 23, 2007 ###
#


###############################################################################
############################# BEGIN FTP ACCESS ################################


sub ftpConnect {
    my ($ftp_site,$ftp_user,$ftp_pass) = @_;
    $LogType = "FTP";
    if (defined $ftp_handle) { &ftpDisconnect; }
    $ftp_handle = Net::FTP->new($ftp_site,Debug=>0,Passive=>0) or do {
    $logger->error("Could not open ftp site: $ftp_site");
    return 0; };
    $ftp_handle->login($ftp_user,$ftp_pass) or do {
    $logger->error("Could not connect with $ftp_user/$ftp_pass to FTP:$ftp_site");
    return 0; };
    $logger->info("FTP Connection open ($ftp_site).");
    $ftp_handle->binary();
    return 1;
}

sub ftpChangeDir {
	my $ftp_dir = shift;
    $LogType = "FTP";
    my $rv = $ftp_handle->cwd("$ftp_dir");
    $logger->info("Current FTP dir is $ftp_dir");
    return $rv;
}

sub ftpDirList {
    my $filter = shift || "";
    $LogType = "FTP";
    my @ftp_dirlist = $ftp_handle->ls("$filter") or &Error("email|fatal","Could not get FTP directory listing");
    return @ftp_dirlist;
}

sub ftpGet {
    my ($ftp_file,$local_file) = @_;
    $LogType = "FTP";
    $logger->info("$ftp_file --> $local_file");
    my $rv = $ftp_handle->get("$ftp_file","$local_file");
    return $rv;
}

sub ftpDisconnect {
    $ftp_handle->quit;
    $LogType = "FTP";
    $logger->info("FTP Connection Closed.");
}


############################ END FTP ACCESS ###################################
###############################################################################


