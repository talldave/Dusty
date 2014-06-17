#!/usr/local/bin/perl
require "is_data_tools_strict.pl";
$|=1;
1;

my %opt = &ReadCommandLine('t');
&setOption("log","NONE");
&setOption("email","OFF");
&BeginProgram;
my $tab = "NAVTECH";

&sqlConnect("n_system");
&sqlConnect("h_system");
&sqlConnect("e_system");

my $get_space = qq{ select
   fs.tablespace_name                          "Tablespace",
   -- (df.totalspace - fs.freespace)              "Used MB",
   fs.freespace                                "Free MB",
   df.totalspace                               "Total MB",
   round(100 * ((df.totalspace - fs.freespace) / df.totalspace)) "Pct. Used"
from
   (select
      tablespace_name,
      round(sum(bytes) / 1048576) TotalSpace
   from
      dba_data_files
   group by
      tablespace_name
   ) df,
   (select
      tablespace_name,
      round(sum(bytes) / 1048576) FreeSpace
   from
      dba_free_space
   group by
      tablespace_name
   ) fs
where
   df.tablespace_name = fs.tablespace_name and df.tablespace_name like '$opt{t}\%'
};

my $result_arrayn = &readQueryMultiRow("$get_space","n_system");
my $result_arrayh = &readQueryMultiRow("$get_space","h_system");
my $result_arraye = &readQueryMultiRow("$get_space","e_system");

print "\n\n";
print "TABLESPACE\tMBfree\tMBtot\tPCTused\n";
print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n";
print "NUTRIA:\n";
foreach my $row (@$result_arrayn) {
	my $tab = @$row[0]; my $free = @$row[1]; my $total = @$row[2]; my $pct = @$row[3];
	print "$tab\t$free\t$total\t$pct\n";
}
print "\n\n";
print "HELIOS:\n";
foreach my $row (@$result_arrayh) {
	my $tab = @$row[0]; my $free = @$row[1]; my $total = @$row[2]; my $pct = @$row[3];
	print "$tab\t$free\t$total\t$pct\n";
}
print "\n\n";
print "EOS:\n";
foreach my $row (@$result_arraye) {
	my $tab = @$row[0]; my $free = @$row[1]; my $total = @$row[2]; my $pct = @$row[3];
	print "$tab\t$free\t$total\t$pct\n";
}



&EndProgram;
