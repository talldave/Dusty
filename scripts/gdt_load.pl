#!/usr/local/bin/perl
require "is_data_tools_strict.pl";

$opt{"proj"}="GDT";
&defineArgs("o","append|truncate_append|dropnswap","Target layer options");
my %opt = &ReadCommandLine('upd:file:o');
&requiredOptions('file:o:upd');
my $gdtlog = "$ENV{LOGHOME}/gdt_"."$opt{file}"."$opt{upd}.$$".".log";
my $logger = &initLogger;
$logger->info("sde83load logfile= $gdtlog");

&BeginProgram;

$gdtdata = "$ENV{GDTDATA}";
my ($server,$inst,$user,$pass) = &getConnectionInfo;

$opt{file} =~ m/^(....)_(.*).txt$/; my $dvd = "$1"; my $dtype = "$2";

open(LAYERLIST,"$opt{file}") or &Error(-1,"File $opt{file} does not exist.");
chdir("$gdtdata/$dvd") or &Error(-1,"Dir $gdtdata doesnt exist");

while (<LAYERLIST>) {
    
    if (-e "stopload") { die "File stopload found.\n"; }
    while (-e "pauseload") { sleep(20); }
    
    if (! /^#/) {
    chop;
    (@lcf) = split(/\,/);
    $layer = shift(@lcf);
    &setOption("l","$layer");
    if ("$opt{o}" eq "dropnswap") { $layer = "$layer"."_NEW"; } 
    if ("$opt{o}" eq "truncate_append") { 
        my $rv = &truncateLayer("$layer"); 
        if ($rv != 0) { &Error(-1,"Truncate failed.  Skipping load"); next; }
    
    }
    
    if ("$layer" =~ /(STREET|LOCAL_ROAD)/i) {
        $key = "GDT_STREET_DEFAULTS";
    } else {
        $key = "GDT_DEFAULTS";
    }
    


    @files = (<load/$dtype/$lcf[0]*.lcf>);
    $pwd = `pwd`;

    foreach my $file (@files) {
        open(IN,"$file") or &Error(-1,"Couldnt open IN file: $file");
        open(OUT,">$file.esri") or &Error(-1,"Couldnt open OUT file: $file.esri");

        while (<IN>) {
            if (/host =/)          { print OUT "host = $server\n"; }
            elsif (/instance =/)   { print OUT "instance = $inst\n";}
            elsif (/schema =/)     { print OUT "schema = gdt\n";}
            elsif (/user =/)       { print OUT "user = $user\n";}
            elsif (/password =/)   { print OUT "password = $pass\n";}
            elsif (/column =/)     { print OUT "column = shape\n";}
            elsif (/table =/)      { print OUT "table = $layer\n";}
            elsif (/keyword = /)   { print OUT "keyword = $key\n";}
            else { print OUT;}
        }  #end while IN
        close(IN);
        close(OUT);
        &ExecCmd("../dvd1/bin/sde91load $file.esri","$gdtlog");
    }  # end foreach file

    #&alterReg("$layer");
    &updateStats("$layer");
    #&grantADOL("$layer");
    &updateMetadata("$layer");
    &getConnectionInfo;


    #&MailMe('dbianco@esri.com,lfitzpatrick@esri.com',"GDT Load FINISHED","Update: $opt{upd}\nDir: $gdtdata\nLogfile: $gdtlog\n");
    }  # end if line not commented out
}  # end while LAYERLIST
close(LAYERLIST);
&EndProgram;

