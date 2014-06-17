#!/usr/local/bin/perl -w
use strict;
require "is_data_tools_strict.pl";

my ($f_table,$t_table);
&defineArgs("s","profile","Source profile");
&defineArgs("t","profile","Target profile");
&defineArgs("o","create|append|init","Target layer options");
&defineArgs("M","minID","Minimum ID");
&defineArgs("tl","layer","Target Layer");
&defineArgs("k","keyword","DBTUNE keyword");

my %opt = &ReadCommandLine('pre:post:s:t:o:file:M:tl:k');
&requiredOptions('s:t:o');
&setOption("p","$opt{t}");
&BeginProgram;
my $logger = &initLogger;
$opt{o} =~ tr/[A-Z]/[a-z]/;
if ("$opt{o}" eq "truncate_append") { &Error("fatal","\t-o truncate_append no longer supported.  Use -o init"); }

if (!defined $opt{post}) { 
	$opt{post} = ""; 
} else {
	$opt{post} = "_"."$opt{post}";
}

if (!defined $opt{pre}) { 
	$opt{pre} = ""; 
} else {
	$opt{pre} = "$opt{pre}"."_";
}

if (!defined $opt{M}) { $opt{M} = 1; }

&sqlConnect("$opt{s}"); &sqlConnect("$opt{t}"); 

my ($server,$inst,$user,$pass,$database,$key) = &getConnectionInfo("$opt{s}"); 
print "DATABASE: $database\n\n";
my $f_serv = "$server"; my $f_inst="$inst";my $f_user="$user";my $f_pass="$pass"; my $f_database="$database";

my ($server2,$inst2,$user2,$pass2,$database2) = &getConnectionInfo("$opt{t}"); 
my $t_serv = "$server2"; my $t_inst="$inst2";my $t_user="$user2";my $t_pass="$pass2"; my $t_database="$database2";

my $shape = "shape"; my $key = "$opt{k}" || "$t_user"."_defaults";

if (defined $opt{file}) {
	open (IN,"$opt{file}") or die;
	while (<IN>) {
		chop;
		s/ //g;
		if (/,/) { 
			m/(.*),(.*)/;
			$f_table = "$1";
			$opt{M} = "$2";
		} else {
			$f_table = "$_";
			$opt{M} = 1;
		}
		if (defined $opt{tl}) {
			$t_table = "$opt{pre}"."$opt{tl}"."$opt{post}";
		} else {
			$t_table = "$opt{pre}"."$f_table"."$opt{post}";
		}
		&setOption("l","$t_table");
		if ("$f_table" !~ /^\#/) { &expimp; }
	} 
	close(IN);
	
} else {
	$f_table = "$opt{l}";
	if (defined $opt{tl}) {
		$t_table = "$opt{pre}"."$opt{tl}"."$opt{post}";
	} else {
		$t_table = "$opt{pre}"."$f_table"."$opt{post}";
	}
	&expimp;
}

&EndProgram;


sub expimp {
	my ($att,$pct);
	if (-e "$t_table.att") { $att = "file=$t_table.att"; } else { $att = "all"; }
	if ("$opt{o}" eq "create") {
		&deleteLayer("$t_table");
		my $sdeexport = "sdeexport -o create -l $f_table,$shape -f - -s $f_serv -i $f_inst -u $f_user -p $f_pass -a $att -w \"$f_table.$shape < 0\"";
		if ("$f_database" ne "") { $sdeexport .= " -D $f_database "; }
		my $sdeimport = "sdeimport -o create -l $t_table,$shape -f - -s $t_serv -i $t_inst -u $t_user -p $t_pass -k $key -M $opt{M}";
		if ("$t_database" ne "") { $sdeimport .= " -D $t_database "; }
		&ExecCmd("$sdeexport | $sdeimport");
		
		my $objid = &readQuerySingle("select rowid_column from sde.table_registry where table_name = upper('$f_table')");
		if (!defined $objid) { &alterReg("$t_table","$opt{M}"); }
	} elsif("$opt{o}" eq "init") {
		&truncateLayer("$t_table");
	}
	
	&load_only_io("$t_table");

	my $sdeexport2 = "sdeexport -o create -l $f_table,$shape -f - -s $f_serv -i $f_inst -u $f_user -p $f_pass -a $att ";
	if ("$f_database" ne "") { $sdeexport2 .= "-D $f_database "; }
	my $sdeimport2 = "sdeimport -o append -l $t_table,$shape -f - -s $t_serv -i $t_inst -u $t_user -p $t_pass ";
	if ("$t_database" ne "") { $sdeimport2 .= "-D $t_database "; }
	&ExecCmd("$sdeexport2 | $sdeimport2");
	
	&normal_io("$t_table");

	my $transCount = &getCount("transfer_count");
	my @fcontent = &getLastCmd;
	my $pid = &getPID;

	my $srcCount = &getLayerCount("$f_table","$opt{s}");
	&getConnectionInfo("$opt{t}");
	my $tgtCount = &getCount("$t_table","$opt{t}");
	
	if ("$srcCount" != 0) { $pct = ($transCount / $srcCount) * 100; } else { $pct = 0; }
	$logger->info("*-*-* $transCount of $srcCount features loaded.  ($pct\%)");
	
#	my $sql = qq{ insert into is_count values (is_count_seq.nextval, $pid,'sde','$opt{s}.$f_table','$opt{t}.$t_table',$srcCount, $tgtCount,$transCount,sysdate,'@fcontent') };
#	&writeQuery("$sql","sdeadmin");

	if (defined $opt{ul}) {
		&updateStatsLarge("$t_table");
	} else {
		&updateStats("$t_table");
	}
	
	&grantADOL("$t_table");
	&getConnectionInfo("$opt{t}");
	&updateMetadata("$t_table");

	
#	my $sql = qq{ insert into is_copysde values (is_copysde_seq.nextval,'$opt{s}','$opt{t}','$t_table',$tgtCount,$tgtSize,$starttime,$stoptime,$elapsedtime,$success_flag) };

	

}

__END__
