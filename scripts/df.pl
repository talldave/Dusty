#!/usr/local/bin/perl

if (defined $ARGV[0]) {
	system "rsh $ARGV[0] df -k > /tmp/dfoutput.txt\n";
} else {
	system "df -lk > /tmp/dfoutput.txt\n";
}
system "sort /tmp/dfoutput.txt > /tmp/dfoutputsorted.txt\n";
open(DF,"/tmp/dfoutputsorted.txt");
while (<DF>) {

chop;
($fs,$bl,$u,$a,$c,$m) = split(/\s+/);

$avail = $a / 1024;
$avail = sprintf("%.2f",$avail);
#print "$m $u/$bl $c $a $avail Mb\n";
print "$m\t\t\t$c\t$avail Mb\n";


}

close(DF);
unlink("/tmp/dfoutput.txt");
unlink("/tmp/dfoutputsorted.txt");

