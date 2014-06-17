#!/usr/local/bin/perl -w
#$ENV{"ISDT_EMAIL"} = "dbianco\@esri.com";
require "is_data_tools_strict.pl";
use File::Basename;
use Net::SSH::Perl;
#
## ADD shapefile, sde list options
## Compare/QC Feature Counts

my %opt = &ReadCommandLine('file:s:i');
&requiredOptions('file');
BeginProgram();
my ($serv,$inst,$user,$pass) = getConnectionInfo();
$user = uc($user);
my $stamp = "1";
my $logf = "cachecmd_$stamp.log";
my $server = "sonoma";
if (defined $opt{s}) { $server = "$opt{s}"; }
my $port = 19195; # port number
if (defined $opt{i}) { $server = "$opt{i}"; }
my $log;
my $logger = initLogger();

open(IN,"$opt{file}") or Error("fatal","Could not open file: $opt{file}");
open(OUT,">$opt{file}.tmp");
print OUT "SDEWORKSPACE sde-1 5151 198.102.35.55 adol adol\n";
while (<IN>) {
	chop;
	my $layer = "$_";
	my %desc = describeLayer("$layer");
	my $type;
	if ("$desc{entities}" =~ /p/) { 
		$type = 0;
	}elsif ("$desc{entities}" =~ /(l|s)/) {
		$type = 1;
	} else { $type = 2; }
	print OUT "DELETE $user.$layer\n";
	print OUT "LOAD FCLASS $user.$layer $type\n";
} #end while IN
close(IN);
print OUT "disconnectworkspace sde-1\n";
close(OUT);

my $rv = ExecCmd("java -jar /mangomap1/users/sde/bin/cachecmd.jar $opt{file}.tmp $server $port","$logf");

#Log file was created: sonoma//sonoma2/disk1/cache/dl/sl/log/200605151136.log
open(IN,"$logf");

while (<IN>) { chop; if (/^Log file was created: $server\/(.*)/) { $log = "$1";}  }

$logger->info("Remote logfile: $log");
close(IN);


my $verbose=0;
my $wait = 1;  # seconds to wait before first check

my $result;
until (&check_log_file) {
	$logger->info("INFO: Still not finished. Trying again in $wait seconds ... (",time - $^T," seconds running)");
	sleep $wait;
	$wait *= 2 unless $wait > 600; # Check at least every 10 minutes
	die "Giving up\n" if time - $^T > 172800; # Give up (silently) after 48 hours
} continue {
	check_log_file();
	print "Continuing $wait\n";
}
$logger->info("Cache load complete");

my @lines = grep /LOAD.*sec/, split(/\n/, $result );
foreach (@lines) {s/LOAD //;s/ / : /;s/  / features - /;s/sec\./ sec./;$logger->info("$_");}
my @errors = grep /ERRORS|Exception/, split(/\n/, $result );
if (scalar @errors > 1 ) {
	$logger->info("POTENTIAL PROBLEMS");
	foreach (@errors) {s/LOAD //;s/ / : /;s/  / features - /;$logger->info("$_");}
}
my $secs=time() - $^T;
$logger->info("INFO: Script $0 finished in $secs seconds.");
$logger->info("INFO: Logfile: $log");
$logger->info("INFO: ssh unix\@$server 'cat $log'");

####################################################################

sub check_log_file {
	#my $ssh = Net::SSH::Perl->new("$server", protocol=>2);  # , debug => 1, protocol=>2
	#my $usr = 'unix';	my $pwd = 'some.where';
	my $cmd = qq(cat $log);
	#$ssh->login("$usr","$pwd");
	##print qq(INFO: Checking '$cmd' on $server\n) if $verbose;
	#($result, my $stderr, my $exit) = $ssh->cmd("$cmd");
	#print "stderr=$stderr\n" if $stderr;
	#print "exit=$exit\n" if $exit;
	#return $result;

	my $done=0;
	my $rv = system "ssh unix\@$server $cmd > $opt{file}.log";
	system "cat $opt{file}.log";
	open(IN2,"$opt{file}.log");
	while (<IN2>){
		if (/Time to Complete/i) { $done=1; }
	}
	close(IN2);
	return $done;
}

#unlink("$opt{file}.tmp");

EndProgram();

__END__


