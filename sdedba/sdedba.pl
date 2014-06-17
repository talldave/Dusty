#!/usr/local/bin/perl -w 
use strict;

#
# sdedba
#
# Description:
#
# Author: David Bianco, ESRI
#
# Usage:
#
# History:  
#

############################################
############ GLOBAL VARIABLES ##############

my $LOGHOME = "$ENV{LOGHOME}";
my $EXIT_VALUE = 0;
my $SQL_ERROR = undef;
$|=1;
my $ExecCmdCount=0;
my ($lastCmd_log);
my $Caller = "Begin";
my $LogType = "INFO";
my ($errLog,$errEmail,$errScreen);
$errLog=$errEmail=$errScreen=0;
my $pid = $$;
my $pwd = `pwd`; chop($pwd);
my $hostname = `hostname`; chop($hostname);
#my $boldon = `tput smso`; my $boldoff = `tput rmso`;
my ($parselist,@orgARGV);
my (%arg,%arg_short,$starttime,$starttime_e,$logstamp,$logfile,$logfile_new,$logid);
my ($elapsedtime,$last_stop,$totaltime,$cmd_id);
my (%dbh,$dbf_handle,$ftp_handle);
my ($logger,%logger);
my ($transCount,$TotalCount,$TransferCount);
$transCount=$TotalCount=$TransferCount=0;
my @lastcmdcontents;
my ($sde_connection,$server,$user,$pass,$inst,$database);
my $script = basename($0);
################################################

use Carp;
use DBI;
#require "sdedba_core.pl";

use Text::ParseWords qw(shellwords);
use POSIX qw(strftime);

# # GLOBALS # # 
#my $year = strftime("%Y", localtime((stat $0)[9]));
#my $week = strftime("%w", localtime((stat $0)[9]));
 my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime((stat $0)[9]);
 $year-=100;
my $VERSION = ((($year - 3)*1000) + $yday)/1000;
my $LAST_BUILD_DATE = strftime("%b %d, %Y %H:%M %p", localtime((stat $0)[9]));
my @COMMAND;  # Array to store input from SDEDBA prompt 
my @HISTORY;  # Array to store history of commands typed
my $PROFILE;
my ($SERVER,$INSTANCE,$USER,$PASS,$SDE_CONNECTION);
my $KEYWORD = "defaults";
#my %PROFILE;
my $PROJECT;
my $PROJECT_VERSION;
my $CMDFILE_LINENUMBER;
my $LAST_LIST_ELEMENT = 1; # default is TRUE
my $NAME;  # Current Layer/Table/View/Raster/File name
my $DNAME; # Display name in form SERVER.USER.NAME
my $TYPE;  # Type of Current name (layer,table,view, ...)
my $SQL;  # Current SQL
my $VERBOSE; # level of output (0=nothing,1=minimum,2=maximum)
my $MSG1;  # Message to be logged if VERBOSE is 1
my $MSG2;  # Message to be logged if VERBOSE is 2
my $MSG;   # Message to be logged regardless of VERBOSE level


my %opt = &ReadCommandLine('inv:xml:cmd:v:q:proj');
&setOption("email","OFF");

&BeginProgram;  # setup logger, global vars

&title;
if (defined $opt{v}) { &set("verbose","$opt{v}"); }
if (defined $opt{h}) { 
	&Usage; 
} elsif (defined $opt{cmd}) {
	&cmd_RUN("$opt{cmd}");
	&recieveInput;
} else {
	&recieveInput;
}
#if (defined $opt{inv}) 		{ &inventory("$opt{proj}","$opt{inv}"); }
#elsif (defined $opt{cmd}) 	{ &parseCMD("$opt{cmd}"); }
#elsif (defined $opt{xml}) 	{ &parseXML("$opt{xml}"); }
#elsif (defined $opt{h}) 	{ &Usage; }
#else 				{ &sdedba_prompt; }

&EndProgram;


sub cmd_RUN {
	open(CMD_FILE,"$opt{cmd}") or &Error("fatal","Could not open file $opt{cmd}");
	$MSG = "Parsing command file $opt{cmd} ...\n";
	&log;
	while (<CMD_FILE>) 
	{
		chop;
		if (! /^\s*\#/) 
		{
			my $input = "$_";
			# insert test for end of line comment (e.g.  load data # this loads data)
			&cmdProcessor("$input");
		}
	} # end while CMD_FILE
	close(CMD_FILE);
	undef($opt{cmd});	
} # end sub cmd_RUN

sub recieveInput {
	my $input="begin";
	my $is_profile_valid=0;
	if (defined $PROFILE) { $is_profile_valid = 1; }

	
	while  (! $is_profile_valid) 
	{
		print "\nEnter connection profile: ";
		chomp($PROFILE=<STDIN>);
		if ("$PROFILE" =~ /^\s*$/) { $PROFILE="No_Profile_Set"; last; } # if "return"
		#$COMMAND[0]="$PROFILE";
		if ("$PROFILE" =~ /^\s*(quit|exit)/i) { print "\n\n\n"; exit(0); }

		print "\n";
	#	$profile = &validateProfile("$PROFILE");
		#$is_profile_valid = &getConnectionInfo("$PROFILE");
		$is_profile_valid = &set("profile","$PROFILE");
	}

	while ("$input" !~ /^QUIT/i) 
	{
		my $prompt_time = strftime("%H:%M:%S", localtime());
		my $prompt = qq{\n($PROFILE)($prompt_time) SDEDBA> };
		print "$prompt";
		chomp($input=<STDIN>); # wait for user input
		$input =~ s/^(\s*)//;
		&cmdProcessor("$input");
	} # end while input != quit
} # end sub recieveInput
	
	




sub cmdProcessor {
	my $input = "@_";
	my $cmd_args;
	my @cmd_args;
	@COMMAND = shellwords("$input");

	if (($#COMMAND == 0) and 
	    ("$COMMAND[0]" =~ /(LOAD|COPY|EXPORT|GROUP|GRID|VIEW)/i)) 
	{ 
		&cmd_Usage("$COMMAND[0]"); 
		next; 
	}
	push(@HISTORY,"$input");
	if ("$input" =~ /^\s*$/) { return; } # if user hit return, show prompt again
	$SIG{INT} = sub { print "\nCtrl-C exit\n\n\n"; exit(0); }; # if CTRL-C then exit

	my $cmd0 = "$COMMAND[0]"; $cmd0 .= " ";

if ("$cmd0" =~ /(load |copy |export |group |index |delete |normal )/ ) {
	if ("$#COMMAND" > 0) { $NAME = "$COMMAND[1]"; } else { $NAME=""; }
	print "NAME= $NAME\n";
	if (("$NAME" !~ /\./) && (defined $SERVER)) { $DNAME = "$SERVER"."."."$USER"."."."$NAME"; } else { $DNAME = "$NAME"; }
	

	if ("$NAME" =~ /\.(SHP|SDX|LCF)$/i) 	{ $TYPE = "LAYER"; }
	elsif ("$NAME" =~ /\.(DBF|CSV)$/i) 	{ $TYPE = "TABLE"; }
	elsif ("$NAME" =~ /\.(TIF|BSQ|JPG)$/i) 	{ $TYPE = "RASTER"; }
	else { 
		my $sql;my @ans;$TYPE="";
		print "N= $NAME .. T= $TYPE\n";
		until ("$TYPE" ne "") {
					print "N= $NAME .. T= $TYPE\n";

			$sql = qq{ select table_name from sde.layers where table_name = upper('$NAME') and owner = upper('$USER') };
			print "sql= $sql\n";
			@ans = &readQuerySingle("$sql");
			if ("$ans[0]" ne "") { $TYPE = "LAYER"; }
		print "N= $NAME .. T= $TYPE\n";

			$sql = qq{ select table_name from gdb_rastercolumns where table_name = upper('$NAME') and owner = upper('$USER') };
			@ans = &readQuerySingle("$sql");
			if ("$ans[0]" ne "") { $TYPE = "RASTER"; } 
					print "N= $NAME .. T= $TYPE\n";

		} #end until
	} # end if NAME
} # end if cmd0
		print "N= $NAME .. T= $TYPE\n";

	$MSG2 = "@COMMAND\n"; &log;
	
	SWITCH: for ($cmd0) {
		/^LOAD /i 		&& do { &cmd_LOAD; last; };
		/^COPY /i 		&& do { &cmd_COPY; last; };
		/^EXPORT /i 		&& do { &cmd_EXPORT; last; };
		/^GROUP /i 		&& do { &cmdCOPY; last; };
		/^GRID /i 		&& do { &cmdCOPY; last; };
		/^NORMAL /i 		&& do { &cmdCOPY; last; };
		/^DISSOLVE /i 		&& do { &cmdCOPY; last; };
		/^LARGE /i 		&& do { &cmdCOPY; last; };
		/^GRANT /i 		&& do { &cmd_GRANT; last; };
		/^REGISTER /i 		&& do { &cmdCOPY; last; };
		/^PROJECTION /i		&& do { &cmdCOPY; last; };
		/^ANALYZE /i 		&& do { &cmd_ANALYZE; last; };
		/^DELETE /i		&& do { &cmd_DELETE; last; };
		/^INDEX /i 		&& do { &cmdCOPY; last; };
		/^VIEW /i 		&& do { &cmd_VIEW; last; };
		/^DROPNSWAP /i 		&& do { &cmdCOPY; last; };
		/^DESCRIBE /i 		&& do { &cmdCOPY; last; };
		/^PROFILE /i 		&& do { &cmd_PROFILE; last; };
		/^LIST /i 		&& do { &cmdCOPY; last; };
		/^LASYERLIST /i		&& do { &cmdCOPY; last; };
		/^SHOW /i 		&& do { &cmdCOPY; last; };
		/^EMAIL /i 		&& do { &cmd_EMAIL("$COMMAND[1]"); last; };
		/^USAGE /i 		&& do { &cmd_Usage("$COMMAND[1]"); last; };
		/^RUN /i 		&& do { $opt{cmd} = "$COMMAND[1]"; &cmd_RUN("$opt{cmd}"); last; };
		/^SPOOL /i 		&& do { &cmdCOPY; last; };
		/^HISTORY /i 		&& do { &cmd_HISTORY; last; };
		/^QUIT /i 		&& do { &EndProgram; };
		/^(SELECT|INSERT|UPDATE|DELETE FROM|DROP|CREATE|ALTER|ANALYZE TABLE|ANALYZE INDEX) /i  
					&& do { &cmd_SQL; last; };
		/^\@/ 		&& do { &cmd_SQL; last; };
		/^SQL /i  		&& do { &cmd_SQL; last; };  # instead of above??
		#/^\s*(\!|HOST )/i	&& do { $input =~ s/^(\!|HOST )//i; system "$input"; last; };
		/^\!(\d+)/i		&& do { my $n = "$1"; $n--; $input = "$HISTORY[$n]"; print "$input\n"; next; };

		/^CMD /i		&& do { $input =~ s/^CMD //i; system "$input"; last; }; # instead of above??
		/^SET /i 		&& do { &set("$COMMAND[1]","$COMMAND[2]"); last; };

		$input =~ s/ $//; 
		#print "error: unknown command \"$input\"\n";
		system "$input";
	} # end SWITCH
	return 1;
} # end sub cmdProcessor

sub validateProfile {
	
} # end sub validateProfile

sub cmd_SQL {
	&sqlConnect;
	&doQuery("@COMMAND");
} # end sub cmd_SQL

sub log {
	

	if 	(($VERBOSE == 1) && (defined $MSG1)) { $logger{"SCREEN"}->info("$MSG1"); }
	elsif 	(($VERBOSE == 2) && (defined $MSG2)) { $logger{"SCREEN"}->info("$MSG2"); }

	if (defined $MSG) { $logger{"SCREEN"}->info("$MSG"); $MSG2="$MSG"; }
	

	if (defined $MSG2) { $logger{"LOG"}->info("$MSG2"); }
	
	
	undef($MSG); undef($MSG1); undef($MSG2);
	return 1;
	
} # end sub log

sub set {
	my ($prop,$value) = @_;
	if ((!defined $prop) || (!defined $value)) { $MSG = "Invalid set command.\nset <PROPERTY> <VALUE>\n"; &log; return; }
#	$MSG2 = "@COMMAND\n";
	PROPERTY: for ($prop) {
		/profile/i && do 	{
			($SERVER,$INSTANCE,$USER,$PASS) = &getConnectionInfo("$value");
			$SDE_CONNECTION = "-s $SERVER -i $INSTANCE -u $USER -p $PASS";
			&sqlConnect("$value");
			$PROFILE="$value";
			last;
					};
		/verbose/i && do 	{
			if (($value >=0) && ($value <= 2)) { $VERBOSE = $value; } else { $MSG="Verbose must be 0,1, or 2"; }
			last;
					};
	} # end PROPERTY
	return 1;
} # end sub set

sub show {
	
	
} # end sub show


sub exists {
	my $sql;
	if ("$TYPE" eq "LAYER") { 
		$sql = qq{ select table_name from sde.layers where table_name = upper('$NAME') and owner = upper('$USER') };
		$MSG2="$sql\n";
		my @ans = &readQuerySingle("$sql","$PROFILE");
		if (defined $ans[0]) { return 1; } else { return 0; }
	}
} # end sub exists

sub cmd_REGISTER {
#	my $table = shift;
	my $column = "$COMMAND[1]" || "OBJECTID";
	my $options = "$COMMAND[2]" || "";
	#my ($table,$min_id) = @_;
	#if (!defined $min_id) { $min_id = 1; }
	$MSG1 = "Registering $column column ...   ";
	my $rv = &ExecCmd("sdetable -o alter_reg -t $NAME -c $column -C SDE -L off -H visible -N -V single $sde_connection");
	if ($rv == 0) { $MSG1 = "SUCCESS\n"; }
	&log;
	## ** NEED TO ADD OBJECTID CHECK TO METADATA TABLES (GDB_OBJECTCLASSES) **
}

sub cmd_ANALYZE {
#	my $table = shift;
	my $method = "$COMMAND[1]" || "ESTIMATE";
	$MSG1 = "Analyzing table $DNAME ...   ";
	my $rv = &ExecCmd("sdetable -o update_dbms_stats -t $NAME -m $method $sde_connection -N");
	if ($rv == 0) { $MSG1 = "SUCCESS\n"; }
	&log;
	## automatic update in metadata
	## no need to notify
	## datestamp can be found in dba_tables
}



sub cmd_GRANT {
	my $grant_user = "$COMMAND[1]" || "ADOL";
	$MSG1 = "Granting $grant_user access to $DNAME ...  ";
	my $rv = &ExecCmd("sdetable -o grant -t $NAME -A SELECT -U $grant_user $SDE_CONNECTION");
	if ($rv == 0) { $MSG1 = "SUCCESS\n"; }
	&log;
	## automatic update in metadata
	## no need to notify
} # end sub cmd_GRANT

sub cmd_DELETE {
	my $rv;
	$MSG1 = "Deleting $TYPE $DNAME ...  ";
	


	if ("$TYPE" eq "LAYER") { $rv = &ExecCmd("sdetable -o delete -t $NAME $SDE_CONNECTION -N"); }
	if ("$TYPE" eq "TABLE") { my $sql = qq{ drop table $NAME }; }
	if ("$TYPE" eq "RASTER"){ $rv = &ExecCmd("sderaster -o delete ...."); }
	
	if ($rv == 0) {	$MSG1 = "SUCCESS\n"; } 
	elsif ($rv == -37) { $MSG1 = "** ERROR **\n\tTable not found ($DNAME).  Unable to delete.\n"; }
	&log;
}  # end sub cmd_DELETE



sub cmd_LOAD {
	my ($dummy,$source,$target,$type,$options) = @COMMAND;
	@COMMAND = ("","","");
	$type = uc($type);
	my $source_type = "$source"; 
	$source_type =~ m/\.(shp|dbf|lcf|sdx|csv|tif)$/; 
	$source_type = "$1"; 
	$source_type = uc($source_type);
	if (!defined $options) { $options = ""; }

	if ("$target" =~ /\./) { my ($serv,$user,$target) = split(/\./,"$target");
				 &setOption("p","$serv\_$user"); }

	$NAME = "$target";
	$DNAME = "$SERVER"."."."$USER"."."."$target";
	$KEYWORD = "$USER"."_DEFAULTS";
	

	if ("$source_type" =~ /(SHP|SDX|LCF)/) 	{ $TYPE = "LAYER"; }
	if ("$source_type" =~ /(DBF|CSV)/) 	{ $TYPE = "TABLE"; }
	if ("$source_type" =~ /(TIF|BSQ|JPG)/)  { $TYPE = "RASTER"; }


	if ("$type" eq "CREATE") { &cmd_DELETE if &exists; }
	if ("$type" eq "INIT") 	 { &truncate; }

	$MSG1 = "Loading $source to $DNAME ...  ";

	SOURCE: for ($source_type) {

	/SHP/ && do {
		if ("$type" =~ /(APPEND|INIT)/) {
			&shapeAppendSimple("$source","$target"); # change to ExecCmd
			if ($LAST_LIST_ELEMENT) { &cmd_ANALYZE; }
			
		} elsif ("$type" eq "CREATE") {

			if ("$options" !~ /-a /) {
				if (-e "$source.att") { 
					$options .= " -a file=\"$source.att\"";
				} elsif (-e "$source.none") {
					$options .= " -a none";
				} else {
					$options .= " -a all";
				}
			}
			my $rejectshp = "$source"; $rejectshp =~ s/(...)$//; $rejectshp .= "_reject.shp";
			
			my $rv = &ExecCmd("shp2sde -o create -l $NAME,shape $SDE_CONNECTION -f $source -k $KEYWORD -r $rejectshp $options");

			if ($rv == 0) { $MSG1 = "SUCCESS\n"; }
			&log;
			&cmd_GRANT;
			&cmd_REGISTER;
			&cmd_ANALYZE;
		} else { $MSG= "** ERROR **\n\tInvalid type ($type).  Expected: APPEND, CREATE, or INIT.\n\tFile $opt{cmd} line $.\n"; &log; return; }
		last;
	};

	/DBF/ && do {
	

	};

	/SDX/ && do {
		
	};
	
	/CSV/ && do {
		
	};
	
	/TIF/ && do {
		
	};
	
	} # end SOURCE
	
	&updateMetadata("$target");
	
} # end sub cmd_LOAD




sub cmd_VIEW {
	my ($view_name,$col_list,$alias_list,$table_list,$where_clause,$rv);
	if ($#COMMAND == 0) { 
		print "Enter view name: ";
		chomp($view_name=<STDIN>);
		print "Enter column list: ";
		chomp($col_list=<STDIN>);
		print "Enter alias list: ";
		chomp($alias_list=<STDIN>);
		print "Enter table list: ";
		chomp($table_list=<STDIN>);
		print "Enter where clause: ";
		chomp($where_clause=<STDIN>); 
	} else {
		($view_name,$col_list,$alias_list,$table_list,$where_clause) = "@COMMAND";
	}
	$MSG1 = "Creating view ...\n";
	$rv = &createView("$view_name","$col_list","$alias_list","$table_list","$where_clause");
} # end sub cmd_VIEW



sub cmd_HISTORY {
	my $x=1;
	foreach my $cmd (@HISTORY) {
		print "$x  $cmd\n";
		$x++;
	} # end foreach my $cmd
} # end sub cmd_HISTORY



sub cmd_PROFILE {
	my ($p);
	my $x=0;
	if ($#COMMAND == 0) {
		# list all profiles, asterisk to show current profile
		my $sql = qq{ select profile from is_users order by profile };
			
		my $result_array = &readQueryMultiRow("$sql","sdeadmin");		
		foreach my $row (@$result_array) {
			$x++;
			$p = @$row[0];		
			printf "%-15s", $p;
			if ("$x" == 4) { print "\n"; $x=0; }
		} # end foreach my row
	} else {
		# set profile to $COMMAND[1]
		($SERVER,$INSTANCE,$USER,$PASS) = &getConnectionInfo("$COMMAND[1]");
		$SDE_CONNECTION = "-s $SERVER -i $INSTANCE -u $USER -p $PASS";
		$PROFILE="$COMMAND[1]"; 
		&sqlConnect("$PROFILE");
	} # end if COMMAND args = 0
	return 1;
} # end sub cmd_PROFILE

sub cmd_Usage {
	my $cmd = shift;
	$cmd = uc($cmd);
	print "\nUsage:  $cmd ";
	CMD_USAGE: for ($cmd) {
		/LOAD/i 		&& do { print "<source_file> <target> <append | create | init> <\"options\">\n";
					last; };
		/^COPY /i 		&& do { &cmdLOAD; 		last; };
		/^EXPORT /i 	&& do { &cmdLOAD; 		last; };
		/^GROUP /i 		&& do { &cmdCOPY; last; };
		/GRID/ 			&& do { print "<target> <grid_size>\n"; last; };
		/^NORMAL /i 	&& do { &cmdCOPY; last; };
		/^DISSOLVE /i 	&& do { &cmdCOPY; last; };
		/^LARGE /i 		&& do { &cmdCOPY; last; };
		/^GRANT /i 		&& do { &cmdCOPY; last; };
		/^REGISTER /i 	&& do { &cmdCOPY; last; };
		/^PROJECTION /i	&& do { &cmdCOPY; last; };
		/^ANALYZE /i 	&& do { &cmdCOPY; last; };
		/^INDEX /i 		&& do { &cmdCOPY; last; };
		/VIEW/ 			&& do { 
						print "<view_name> <\"columns\"> <\"alias\"> <\"tables\"> <\"where_clause\">\n"; 
						print "Example: VIEW foo_v \"a.shape,a.objectid,b.column1\" \"\" \"sde_layer a,table b\" \"a.id = b.key\"\n";
						last; 
				};
		/^DROPNSWAP /i 	&& do { &cmdCOPY; last; };
		/^DESCRIBE /i 	&& do { &cmdCOPY; last; };
		/^\s*PROFILE /i && do { &cmd_PROFILE; last; };
		/^LIST /i 		&& do { &cmdCOPY; last; };
		/^LASYERLIST /i	&& do { &cmdCOPY; last; };
		/^SHOW /i 		&& do { &cmdCOPY; last; };
		/^USAGE /i 		&& do { &cmdCOPY; last; };
		/^SPOOL /i 		&& do { &cmdCOPY; last; };
		/^\s*HISTORY /i && do { &cmd_HISTORY; last; };
		/^\s*QUIT /i 	&& do { last; };
		/^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|ANALYZE) /i  
				&& do { &cmd_SQL; last; };
		/^\s*\@/ 	&& do { &cmd_SQL; last; };
		/^\s*SQL /i  	&& do { &cmd_SQL; last; };  # instead of above??
	} # end CMD_USAGE
	print "\n";
} # end sub cmd_Usage


#-------------#

sub title {
	print "\n\n";
	print "[------------------------------------------------------------]\n";
	print "	 sdedba  ver. $VERSION --- $LAST_BUILD_DATE\n";
	print "[------------------------------------------------------------]\n\n";
} # end sub title


sub Usage {
	print <<EOT;
	
	sdedba -h
	sdedba
	sdedba -inv {source|<database>} -out <output_file> -proj <project> 
					-type {shp|dbf|sde|raster|table|all}
	sdedba -xml <SDEDBA_XML_file> 	-v {verbose_level} -q
	sdedba -cmd <SDEDBA_CMD_file> 	-v {verbose_level} -q
	
	
	-h	Print description of options
	-inv 	Inventory a dataset and create a Data Definition File
		source - inventory the source files
		database - name of an SDE database to inventory
	-out 	Name of Data Definition File (default: PROJ_INV_TYPE_dd.xml)
	-proj	Project of dataset to be inventoried
	-type	Specific datatype or "all" (default: all)
		shp	-
		dbf	-
		sde	-
		raster 	-
		csv	-
		cov	-
		table	-
	-xml 	SDEDBA_XML file name
	-cmd	SDEDBA_CMD file name
	-v	Verbose level (default: 1)
		0 - off
		1 - minimum
		2 - maximum
	-q	Quiet.  No execution. Instead, output a csh version of what would happen.
	
	sdedba		- Interactive command prompt mode
	sdedba -inv 	- Inventory dataset.  Create Data Definition File.
	sdedba -cmd 	- Parse SDEDBA_CMD file
	sdedba -xml 	- Parse SDEDBA_XML file
	sdedba.gui	- GUI version (no command-line options)
	sdedba.mnu	- Text menu version (no command-line options)
	
EOT
exit(1);

} # end sub Usage


#############################################################__#############################################
# #!/usr/local/bin/perl -w
# use strict;
use Log::Log4perl qw(get_logger :levels);
#use DBI;
use Net::FTP;
use Mail::Mailer;
use File::Basename;


############################################


sub ReadCommandLine {
        $parselist=$_[0] || "";
        $parselist="$parselist".":l:p:h:log";   ##  Global Command Line Arguments
        my($numArgs)=0;
        @orgARGV=@ARGV;
        my(@CommandLine)=@ARGV;
        my(@ParseList,%ParseRules,@GenericList,$item,$parm,$value);

        (@ParseList)=split(/:/,$parselist);

        foreach $item (@ParseList) {
                $ParseRules{$item}=1;
        }

        while ($parm=shift(@CommandLine)) {
                if ($parm =~ m/^\-([a-zA-Z]+)$/o) {
			$numArgs++;
                        $parm=$1;
                        $opt{$parm}=1;
                        if ($ParseRules{$parm}) {
                                $value=shift(@CommandLine) || "";
                		if (($value =~ m/^\-([a-zA-Z]+)$/o) or ($value eq "")) {
					$opt{$parm}=1;
					unshift(@CommandLine,$value);
                                } else {
                                	$opt{$parm}=$value;
				}
                        }
                        next;
                }
                push(@GenericList,$parm);

        }
        my @ARGV=@GenericList;

      #  if (defined $opt{h}) { &printUsage($parselist); }
        if (defined $opt{log}) { 
		$opt{log} =~ tr/[a-z]/[A-Z]/; 
		if ("$opt{log}" !~ /(SCREEN|NONE|CSH|CRON)/) { 	die "\nPlease use   -log <SCREEN|NONE|CSH|CRON>\n"; }
	} else { 
		$opt{log} = "DUMMY"; 
	}
	if (!defined $opt{email}) { $opt{email} = "$ENV{ISDT_EMAIL}"; }
	if (!defined $opt{sqloutput}) { $opt{sqloutput} = "ON"; }
        return %opt;
}

sub requiredOptions {
	my($parselist2)=$_[0];
	my(@ParseList)=split(/:/,$parselist2);
	my $x=0;
	foreach my $item (@ParseList) {
		if (!defined $opt{$item}) {
			$x=1;
			print "Required option missing: -$item\n"; 
		}
	}
	if ("$x" == 1) { &printUsage($parselist); }
}

sub setOption {
	my($option,$value) = @_;
	if (!defined $value) { $value=1; }
	$opt{$option} = "$value";
	return $opt{$option};
}

sub defineArgs {
	my($option,$short,$long) = @_;
	if (defined $option) { 
		$arg{$option} = "$long"; 
		$arg_short{$option} = "$short"; 
	} else {
	
	$arg{h} 		= "Help";
#	$arg{q} 		= "Quiet operation (only print statements, no execute).";
	$arg{l} 		= "SDE Layer";
	$arg{p} 		= "SDE connection profile.";
#	$arg{proj}		= "Project";
	$arg{file} 		= "Filename of layers to loop through";
#	$arg{sfips} 		= "State FIPS";
	$arg{pre}		= "Prefix to be given to layer name.";
	$arg{post} 		= "Suffix given to new layer name";
	$arg{log} 		= "Specify logfile option.  Output to logfile is default";
	
	$arg_short{l} 		= "layer";
	$arg_short{p}	 	= "profile";
	$arg_short{f} 		= "filename";
#	$arg_short{proj}	= "project";
	$arg_short{file} 	= "file";
#	$arg_short{sfips} 	= "state_fips";
	$arg_short{pre} 	= "prefix";
	$arg_short{post}	= "suffix";
	$arg_short{log} 	= "screen|none|csh|cron";
	
	}
}

sub printUsage {
	my $parselist = "@_"; undef(@_);
	&defineArgs;
        my(@ParseList)=split(/:/,$parselist);

	print "USAGE:\n\n$script ";
	foreach my $flag (@ParseList) {
		if (defined $arg{$flag}) {
			print "-$flag ";
		}
		if (defined $arg_short{$flag}) {
			print "<$arg_short{$flag}> ";
		}
	}
	print "\n\nOptions:\n";
	
	foreach my $flag (@ParseList) {
		print "\t-$flag\t\t$arg{$flag}\n";
	}
	exit(1);
}


sub ExecCmd {
	my $cmd=shift; 
	$lastCmd_log=shift || "$LOGHOME/sdedba_$pid"."_$ExecCmdCount".".tmp";
	if ("$opt{log}" eq "CSH") { print "CMD: $cmd\n"; return 1; }
	$ExecCmdCount++;
	$LogType = "CMD";
	$Caller = (caller(1))[3]; $Caller =~ s/^main:://;
	if ($VERBOSE == 2) { print "\n"; }
	$MSG2 = "$cmd\n"; 
	&log;
	my $a=1;

	while (-e "$ENV{HOME}/pause$pid") { 
		if ("$a"==1) { 
			$logger->info("*-x-* Execution paused *-x-*");
			$logger->info("Remove file $ENV{HOME}/pause$pid to continue."); 
			$a++;
		}
		sleep(10); 
	}
	
	if (-e "$ENV{HOME}/stop$pid") {
		$logger->info("*xxxxx* Execution stopped with file $ENV{HOME}/stop$pid");
		$EXIT_VALUE = -9;
		&EndProgram;
	}

	#if("$opt{log}" eq "SCREEN") { print "\n"; }
	

	#print $boldon;
	#$logger->info("$cmd");
	#print $boldoff;
	
	
	open(SAVEOUT, ">&STDOUT");
	open(SAVEERR, ">&STDERR");
	open(STDOUT,">$lastCmd_log") or &Error("fatal","Can't redirect output to $lastCmd_log");
	open(STDERR, ">&STDOUT") || &Error(-1,"Can't duplicate stdout");
	select(STDERR); $| = 1;     # make unbuffered
	select(STDOUT); $| = 1;     # make unbuffered
	
	my $RETURN_VALUE = system "$cmd\n"; 
	
	close(STDOUT); 
	close(STDERR);
	open(STDOUT, ">&SAVEOUT");
	open(STDERR, ">&SAVEERR");
	close(SAVEOUT);
	close(SAVEERR);
	
	#$logger->debug("return_value = $RETURN_VALUE");
	#my $elapsed_time = &chrono("elapsed");
	#if ("$RETURN_VALUE" == 0) { 
	#	$logger->info("\tSUCCESS $elapsed_time"); 
	#	&lastCmd;
	#} else { 
	#	$logger->error("\t*** ERROR *** $elapsed_time"); 
	#	&lastCmd;
	#}
	my $rv = &lastCmd;
#	&logSummary("$opt{l}","$server","$pid","$caller","$RETURN_VALUE","$elapsed_time","$cmd","ExecCmd");
	
	return($rv);
}

sub getPID { return $pid; }

sub logSummary {
	my ($layer,$server,$pid,$caller,$rv,$elapsed_time,$cmd,$flag) = @_;
	my $sql = qq{ insert into is_log_summary values (sysdate,'$layer','$server',
			$pid,'$caller',$rv,'$elapsed_time','$cmd','$flag') };
	&writeQuery("$sql","sdeadmin");
}


sub chrono {
	my $option = "@_";
	
	my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
	
	$year -= 100;
	$mon++;
	
	$option =~ tr/[a-z]/[A-Z]/;
	
	if ("$option" eq "START") {
		$logstamp = sprintf("%02d%02d%02d",$year,$mon,$mday);
		$starttime_e = time;
		return $logstamp;
	} elsif ("$option" eq "STOP") {
		$totaltime = time - $starttime_e;
		my ($h,$m,$s);
				
		$h=int($totaltime/3600);
		$totaltime=$totaltime-$h*3600;
		$m=int($totaltime/60);
		$s=$totaltime % 60;
			
		$totaltime = sprintf("%02d:%02d:%02d",$h,$m,$s);
		#$logger->info("TOTAL TIME:  $totaltime");
	} elsif ("$option" eq "ELAPSED") {
		if (!defined $last_stop) { $last_stop = $starttime_e; } 
		$elapsedtime = time - $last_stop;
		$last_stop = time;
		my ($h,$m,$s);
		
		$h=int($elapsedtime/3600);
		$elapsedtime=$elapsedtime-$h*3600;
		$m=int($elapsedtime/60);
		$s=$elapsedtime % 60;
		
		$elapsedtime = sprintf("%02d:%02d:%02d",$h,$m,$s);
		return $elapsedtime;
	}
}

sub BeginProgram {
	my($myfile);
	my $level = "$INFO";
	#if ("$opt{log}" =~ /(SCREEN|DUMMY)/) {
	#	print "\nUse: ( touch $ENV{HOME}/pause$pid ) to pause execution.\n";
	#	print "Use: ( touch $ENV{HOME}/stop$pid ) to stop execution.\n";
	#	if ("$opt{log}" ne "NONE") { print "Use: ( logwatch.pl $pid ) to tail logfile.\n"; }
	#	print "\n";
	#}
	#if (defined $opt{l}) { $temp_var = "$opt{l}"; } else { $temp_var = "dummy"; }
	#if (!defined $opt{p}) { &setOption("p","#NO_PROFILE_SET"); }
	#&setOption("l","Profile: $opt{p}");
	
	if (defined $opt{v}) { &set("verbose","$opt{v}"); } else { &set("verbose","1"); }
	if (!defined $opt{l}) { &setOption("l","#NO_LAYER"); }
	if (!defined $opt{file}) { $myfile = "#NO_FILE"; } else { $myfile = "$opt{file}"; }
	
	&initLogger($level);
	#$logger->info("Command= $script @orgARGV \&"); 
	#$logger->info("Server= $hostname");
	#$logger->info("pwd= $pwd");
	#$logger->info("Profile= $opt{p}");
	#$logger->info("Project= #UNDEFINED");
	#$logger->info("File= $myfile");
	#&setOption("l","$temp_var"); 
	#&MailMe("Script started....");

	
	my $SDE_ADMIN = 'sdeadmin/sdeadmin@ferrari';
	$dbh{sdeadmin} = DBI->connect("DBI:Oracle:","$SDE_ADMIN")
	  	or &Error("","Couldn't connect to database using:  $SDE_ADMIN : $DBI::errstr");
	$cmd_id = "$pid"."$logstamp";
	my $objname;

	if (defined $opt{file}) { $objname = "$opt{file}"; } else { $objname = "$opt{l}"; }
	#my $sql = qq{ insert into is_log_command values ($cmd_id,$pid,null,'$script','@orgARGV',0,'$objname',sysdate,null,null,'$opt{p}','logreader.pl $logstamp $pid') };
	#&writeQuery("$sql","sdeadmin");
	#&logSummary("$opt{l}","$opt{p}","$pid","BEGIN","0","00:00:00","$script","unix");

}

sub initLogger {   
	my $level = shift || "$INFO";
	my $logstamp = &chrono("start");
	my ($layout,$screen_layout);
	#my $file_appender; my $stdout_appender;
    	$logger = get_logger("main_logger");
    	$logger{"SCREEN"} = get_logger("screen_logger");
    	$logger{"LOG"} = get_logger("file_logger");
    #	$logger{"MAIN"} = get_logger("main_logger2");
    	Log::Log4perl::Layout::PatternLayout::add_global_cspec('a', sub { return "$TransferCount" });
    	Log::Log4perl::Layout::PatternLayout::add_global_cspec('b', sub { return "$TotalCount" });
    	Log::Log4perl::Layout::PatternLayout::add_global_cspec('e', sub { return "$Caller" });
   	Log::Log4perl::Layout::PatternLayout::add_global_cspec('f', sub { return "$LogType" });
    	Log::Log4perl::Layout::PatternLayout::add_global_cspec('g', sub { return "$opt{l}" });
    	Log::Log4perl::Layout::PatternLayout::add_global_cspec('i', sub { return "$ExecCmdCount" });

	
	if ($VERBOSE > 0) { 
		$screen_layout = Log::Log4perl::Layout::PatternLayout->new("%m"); 
	 	my $stdout_appender =  Log::Log4perl::Appender->new(
		 	"Log::Log4perl::Appender::Screen",
		         name      => "screenlog",
		         stderr    => 0);
		$stdout_appender->layout($screen_layout);
	  	$logger{"SCREEN"}->add_appender($stdout_appender);	
	}
	

	$layout = Log::Log4perl::Layout::PatternLayout->new("[%d][:%P:][%g %r %e:%i %f] %m%n");
	
	my $file_appender = Log::Log4perl::Appender->new(
		"Log::Log4perl::Appender::File",
	       	 name     => "filelog",
		 mode     => "append",
	       	 filename  => "$LOGHOME/sdedba_$logstamp.log");
	$file_appender->layout($layout);
	$logger{"LOG"}->add_appender($file_appender);
	  
  	$logger{"LOG"}->level($level);
  	$logger{"SCREEN"}->level($level);

  	return $logger;
}
  
sub cmd_EMAIL {
	my $recp = shift;
	my $type = 'sendmail';
	my $email = new Mail::Mailer($type);
#	my $recp = "$opt{email}";
	my $layer;
	$MSG1 = "Sending email to $recp ...\n"; &log;

	if (defined $opt{cmd}) { 
		$layer = "$opt{cmd}";
	} else {
		$layer = "";
	}
	#$layer =~ tr/[a-z]/[A-Z]/;
	my $mesg = `logreader_sdedba.pl $logstamp $pid`;

	my $subj = "ISDT: sdedba $layer ($pid:$ExecCmdCount)";
	my %headers = (
			'To' => qq{$recp},
			'From' => qq{$hostname},
			'Subject' => qq{$subj}
		       );

	$email->open(\%headers);

	print $email "\n$script @orgARGV\n\n";
	print $email "PID is $pid\n";
	print $email "Command count is $ExecCmdCount\n";
	print $email "logreader_sdedba.pl $logstamp $pid\n\n-------------------\n\n";
	print $email $mesg;

	$email->close();
	#$logger->info("Email sent:   $subj");
	return 1;
}

sub lastCmd {
	my ($notify) = shift || "SCREEN";
	my $num_lines = 100; my ($line,$success,$rv);
	$notify =~ tr/[a-z]/[A-Z]/;
	undef(@lastcmdcontents);
	$TransferCount=0;
	my $elapsed_time = &chrono("elapsed");
	$rv = 0;
	$LogType = "RV"; 
	$success = "SUCCESS";

	open(LASTCMD,"tail -$num_lines $lastCmd_log |");
	while (<LASTCMD>) { 
		chop;
		if (! /(^(\s*|\t*)$|^ArcSDE|Utility$|^-----------)/ ) { 
			#$logger{$notify}->info("-\t $_"); 
			#$line = "$_"."<BR>";
			#$line =~ s/\'/\'\'/g;
			push(@lastcmdcontents,$_);
		}
		if (/(.*)(features|records) stored/) { $TransferCount="$1"; $TransferCount =~ s/ //g; }
		if (/\(\-(\d*)\)/) { 
			$rv = "-"."$1"; 
			#$LogType = "RV(-$rv)"; 
			$success = "ERROR";
		} 
	}
	close(LASTCMD);
	$TotalCount += "$TransferCount";
	$transCount = "$TransferCount";
	my ($h,$m,$s) = split(/\:/,"$elapsedtime");
	my $seconds = ($h*3600)+($m*60)+$s;
	if ("$seconds" == 0) { $seconds = 1; }
	my $fps = $TransferCount / $seconds;
	$fps = sprintf("%.3f", $fps);

	$MSG2 = "rv=($rv);stat=($success);fp=($TransferCount);tfp=($TotalCount);time=($elapsed_time);fps=($fps);\n";
	$TransferCount=0;
	$LogType = "RESP";
	foreach my $line (@lastcmdcontents) { $MSG2 .= "\t$line\n"; }
	#if ("$opt{log}" eq "SCREEN") { print "\n\n"; }
	#unlink("$lastCmd_log");
	return $rv;
}

sub getLastCmd { return(@lastcmdcontents); }

sub EndProgram {
	my $cmd;
	&chrono("stop");
	my $sql = qq{ update is_log_command set end_time = sysdate, elapsed_time = '$totaltime',count=1 where id = $cmd_id };
	&writeQuery("$sql","sdeadmin");
	$ExecCmdCount="END"; $LogType="INFO";
	
	my ($h,$m,$s) = split(/\:/,"$totaltime");
	my $seconds = ($h*3600)+($m*60)+$s;
	if ("$seconds" == 0) { $seconds = 1; }
	my $fps = $TotalCount / $seconds;
	$fps = sprintf("%.3f", $fps);
	$logger->info("Total Time= $totaltime");
	$logger->info("Total Features= $TotalCount");
	$logger->info("Total Features per Second= $fps");
	foreach my $profile (keys %dbh) { &sqlDisconnect($profile); }	
	foreach my $file (<$LOGHOME/sdedba_$pid*>) { unlink("$file"); }
	print "\n\n---- Disconnecting from sdedba ver. $VERSION ----\n\n";
	
	
#	if ("$opt{log}" eq "CRON") { $cmd = "logreader_cron.pl"; } else { $cmd = "logreader.pl"; }
#	my $logfile = `$cmd $logstamp $pid`;
#	&MailMe("End Of Script. Ran for $totaltime\n\n<LOGFILE>\n$logfile\n\n.\n");
	#&logSummary("$opt{l}","$opt{p}","$pid","--END","$EXIT_VALUE","$elapsed_time","$script","unix");

	exit($EXIT_VALUE);
}

sub is_Error {
	my($rv) = @_;
	if ("$rv" != 0) { &Error("email"); } 
	return $rv;
}

sub Error {
	my($error_value,$error_message) = @_;

# NOTE:  $error_value shoule be "email" or "fatal" or both "email|fatal" or "" (to send to logfile only)


	$error_value =~ tr/[a-z]/[A-Z]/;
	$EXIT_VALUE=1;
	my $msg="";
	
#	if (("$error_value" =~ /LOG/) || ("$errLog" == 1)) {
#		if (defined $error_message) { 
#			$logger{$error_value}->error("$error_message"); 
#		} else {
#			&lastCmd("log");
#		}
#	} 
	
	if (!defined $error_message) {
		if (!defined $SQL_ERROR) {
			open(LASTCMD,"tail -100 $lastCmd_log |");
			while (<LASTCMD>) { 
				chop;
				if (! /(^(\s*|\t*)$|^ArcSDE|Utility$|^-----------)/ ) { $msg .= "$_\n"; }
			} #end while
			close(LASTCMD);
		} else { 
			$msg = "$SQL_ERROR";
		} #end if ! SQL_ERROR
	} else {
		$logger->error("ERROR:   $error_message");
		$msg = "$error_message";
	}
#	if (("$error_value" =~ /SCREEN/) || ("$errScreen" == 1)) {
#		if (defined $error_message) {
#			$logger{"$error_value"}->error("$error_message"); 
#		} else {
#			&lastCmd("screen");
#		}
	#}
	if (("$error_value" =~ /EMAIL/) || ("$errEmail" == 1)) { &cmd_EMAIL($msg); }
	if ("$error_value" =~ /FATAL/) { &EndProgram; }
	
	
}


################################# END GENERIC TOOLS ###########################
###############################################################################

###############################################################################
################################## BEGIN SDE COMMANDS #########################

sub getConnectionInfo {
	my $profile = shift || "$opt{p}";
	my @row;
	#my $profile = "@_";
	#if ("$profile" eq "") { $profile = "$opt{p}"; }
	#if (!defined $profile) { &Error(-1,"Can not retrieve SDE Connection information. No profile specified."); }
	$logger->level($ERROR);
	my $sql = qq{ 	select SERVER,INSTANCE,USERNAME,PASSWORD
			from IS_USERS where PROFILE = lower('$profile') };
	@row = $dbh{"sdeadmin"}->selectrow_array($sql) 
			or do {
				$MSG =  "error: profile ($profile) not found";
				return 0;
			};
	#my @row = &readQuerySingle($sql,"sdeadmin");
	if ("$#row" < 3) { &Error("fatal","Can not retrieve SDE Connection information. Invalid profile ($profile)"); }
	($server,$inst,$user,$pass,$database) = split(/ /,"@row");
	if (defined $database) {
		$sde_connection = "-s $server -i $inst -u $user -p $pass -D $database";
	} else {
		$sde_connection = "-s $server -i $inst -u $user -p $pass";
	}
	$logger->level($DEBUG);
	return(@row);
}


sub updateStatsLarge {
	my $table = shift;
	my $rv;
	&sqlConnect("$opt{p}");
	my $sql = qq{ select layer_id from sde.layers where table_name = upper('$table') and owner = upper('$user') };
	my @layer_id = &readQuerySingle($sql);
	$rv = &ExecCmd("sqlplus $user/$pass\@$server \@updateStatsLarge $user $table");
	if ("$layer_id[0]") {
		$rv = &ExecCmd("sqlplus $user/$pass\@$server \@updateStatsLarge $user F$layer_id[0]");
		$rv = &ExecCmd("sqlplus $user/$pass\@$server \@updateStatsLarge $user S$layer_id[0]");
	}
	
	my $sql2 = qq{ select index_name from user_indexes where table_name = upper('$table') };
	my $resultArray = &readQueryMultiRow($sql2);
	foreach my $row (@$resultArray) {
		my ($index) = @$row;
		$rv = &ExecCmd("sqlplus $user/$pass\@$server \@updateIndexStatsLarge $user $index");
	}

	return $rv;
	## same notes as updateStats
}

sub deleteLayer {
	my $table = shift;
	my $rv = &ExecCmd("sdetable -o delete -t $table $sde_connection -N");
	return $rv;
	## not kept track of yet
}

sub truncateLayer {
	my $table = shift;
	my $rv = &ExecCmd("sdetable -o truncate -t $table $sde_connection -N");
	return $rv;
	## not kept track of yet
}

sub renameLayer {
	my ($layer,$new_layer) = @_;
	my $rv = &ExecCmd("sdetable -o rename -t $layer -T $new_layer $sde_connection -N");
	#my $sql = qq{ update IS_DBA_METADATA set object_name = '$new_layer' 
	#		where object_name = '$layer' and owner = '$user' };
	#&writeQuery($sql);
	return $rv;
	## not kept track of yet
}

sub indexLayer {
	my ($layer,$ind_column) = @_;
	my $rv = &ExecCmd("sdetable -o create_index -t $layer -n $layer\_$ind_column\_ix -c $ind_column -Q -k $user\_defaults $sde_connection");
	&ExecCmd("sqlplus $user/$pass\@$server \@updateIndexStatsLarge $user $layer\_$ind_column\_ix");
	
	return $rv;
	## automatic update in metadata
	## no need to notify
}

sub createTable {
	my ($dbf_file,$table) = @_;
	my $rv = &ExecCmd("tbl2sde -o create -t $table -f $dbf_file $sde_connection -T dBASE -a all -k $user\_defaults ");
	&updateMetadata("$table","tables"); ## notification to metadata
	return $rv;
}

sub createView {
	my ($view_name,$col_list,$alias_list,$table_list,$where_list) = @_;
	my ($alias,$where);
	if ("$alias_list" eq "") { $alias = ""; } else { $alias = qq{-a "$alias_list"}; }
	if ("$where_list" eq "") { $where = ""; } else { $where = qq{-w "$where_list"}; }
	my $rv = &ExecCmd("sdetable -o create_view -T $view_name -t \"$table_list\" -c \"$col_list\" $alias $where  $sde_connection");
	&updateMetadata("$view_name","views"); ## notification to metadata
	return $rv;
}

sub deleteView {
	my $rv = &deleteLayer("@_");
	return $rv;
	## not kept track of yet
}

sub gridLayer {
	my ($layer,$grid) = @_;
	my $rv = &ExecCmd("sdelayer -o alter -g $grid -l $layer,shape $sde_connection -N");
	&ExecCmd("sdelayer -o normal_io -l $layer,shape $sde_connection");
	return $rv;
	## not kept track of yet
}

sub setLayerDesc {
	my ($layer,$desc) = @_;
	my $rv = &ExecCmd("sdelayer -o alter -S \"$desc\" -l $layer,shape $sde_connection");
	#my $sql = qq{ update IS_DBA_METADATA set DBA_NOTES = DBA_NOTES || ':$desc' };
	#&writeQuery($sql);
	return $rv;
}

sub registerLayer {
}

sub unregisterLayer {
}


sub alterRegNone {
	my $table = "@_";
	my $rv = &ExecCmd("sdetable -o alter_reg -t $table -C NONE -N $sde_connection");
	return $rv;
	## this function is depracated
}
sub groupLayer {
	my ($layer,$tilesize,$append);
	$layer = shift; $tilesize = shift; $append = shift || "create";
	my $newlayer="$layer\_gr";
	if (defined $opt{post}) { $newlayer = "$newlayer"."_$opt{post}"; } 
	my $rv = &ExecCmd("sdegroup -o $append -S $layer,shape -T $newlayer,shape -t $tilesize -e ls+ -a none -k $user\_defaults $sde_connection");
	&updateMetadata("$newlayer"); ## notification
	#&setLayerDesc("$layer\_gr","sdegroup -t $tilesize");
	return $rv;
}
#sub dissolveLayer {
#	my ($layer,$column) = @_;
#	my $rv = &ExecCmd("sdegroup -o create -S $layer,shape -T $layer\_ds,shape -t column=$column -e ls+ -c 1000 -a none -k $user\_defaults $sde_connection");
#	&updateMetadata("$layer\_ds"); ## notification
#	#&setLayerDesc("$layer\_ds","sdegroup -t column=$column");
#	return $rv;
#}
sub largeLayer {
	my ($layer,$size) = @_;
	my $newlayer="$layer\_lg";
	#if (defined "$opt{post}") { $newlayer = "$newlayer"."_$opt{post}"; }
	my $rv = &ExecCmd("sdeexport -o create -l $layer,shape -a all -f - -w \"shape.area > $size\" $sde_connection | sdeimport -o create -l $newlayer,shape -f - $sde_connection -k $user\_defaults");
	&updateMetadata("$newlayer");
	#&setLayerDesc("$layer\_lg","Large version of $layer: -w shape.area > $size");
	return $rv;
}

sub getCount {
	my $layer  = shift; 
	my $handle = shift || "$opt{p}";
	if ("$layer" =~ /transfer_count/i) { return $transCount; }
	#&sqlConnect("$handle");
	my $sql = qq{ select count(0) from $layer };
	my @features = &readQuerySingle("$sql","$handle");
	return $features[0];
}

sub describeLayer {
	my $layer = shift;
	my $tmpfile = "$ENV{ISDT_TEMP}/desc_$layer.tmp";
	my ($att,$val);
	my %desc;
	&ExecCmd("sdelayer -o describe_long -l $layer,shape $sde_connection","$tmpfile");
	open(DESCLAYER_TEMPFILE,"$tmpfile") or &Error("Couldn't open file: $tmpfile");
	while (<DESCLAYER_TEMPFILE>) {
		#print;
		chop; 
		if ((/: /) && (! /^(Offset|Spatial Ind|Layer Envelope)/)) {
			if (/^\s+(minx|maxx)/) {
				m/^\s+(.*):\s+(.*),\s+(.*):\s+(.*)/;
				$desc{"$1"}="$2";
				$desc{"$3"}="$4";
			} else {
				m/^\s*(.*)\b.*:\s+(.*)\s*$/;
				$att="$1";$val = "$2";
				$att =~ tr/[A-Z]/[a-z]/;
				$att =~ s/ /_/g; $att =~ s/\///g;
				#print "$att = $val\n";
				$desc{"$att"}="$val";
			}
		}
	}
	close(DESCLAYER_TEMPFILE);
	unlink("$tmpfile");
	
	if (defined $desc{array_form}) { ($desc{grid},$desc{grid1},$desc{grid2}) = split(/\,/,"$desc{array_form}"); }
	return %desc;
} #end sub describeLayer#

sub exportLayer {
	my ($layer,$export_type) = @_;
	my ($rv,$file_type,$etype);
	my $tmpfile = "shp_entity.tmp";
	my @ans;
	$export_type =~ tr/[a-z]/[A-Z]/;
	if ("$export_type" eq "SDX") {
		$rv = &ExecCmd("sdeexport -o create -l $layer,shape -a all -f $layer.sdx $sde_connection");
	} elsif ("$export_type" eq "SHP") {
		&ExecCmd("sdelayer -o describe_long -l $layer,shape $sde_connection > $tmpfile");
		open(ENTITY_TMP,"$tmpfile") or &Error("Couldnt open $tmpfile");
		while (<ENTITY_TMP>) {
			if (/^Entities .............: (.*)$/) { $etype = "$1"; }
		}
		close(ENTITY_TMP);
		unlink("$tmpfile");
		
		if ("$etype" =~ /(l|s)/) { $file_type = "arc";     }
		if ("$etype" =~ /a/) 	 { $file_type = "polygon"; }
		if ("$etype" =~ /p/) 	 { $file_type = "point";   }

		$rv = &ExecCmd("sde2shp -o init -l $layer,shape -a all -f $layer -t $file_type $sde_connection");
	}
	return $rv;
}

sub importLayer {
	my $layer = "@_";
	my $sde_layer = "$layer"; $sde_layer =~ s/\.sdx//i;
	if (!defined $opt{M}) { &setOption("M","1"); }
	my $rv = &ExecCmd("sdeimport -o create -l $sde_layer,shape -f $layer -M $opt{M} -k $user\_defaults $sde_connection");
	&updateMetadata("$layer");
	return $rv;
}

sub export_import {
	my ($src,$tgt,$arg,$layer,$newlayer) = @_;
	$arg =~ tr/[A-Z]/[a-z]/;
	my ($sde_x,$sde_i,$att);
	$newlayer = $layer unless defined $newlayer;
	my ($f_serv,$f_inst,$f_user,$f_pass) = &getConnectionInfo("$src");
	my ($t_serv,$t_inst,$t_user,$t_pass) = &getConnectionInfo("$tgt");
	if (!defined $opt{M}) { &setOption("M","1"); }

	if ("$arg" eq "create") {
		&deleteLayer("$newlayer");
		$sde_x = "sdeexport -o create -l $layer,shape -f - -s $f_serv -i $f_inst -u $f_user -p $f_pass -a $att -w \"$layer.shape < 0\"";
		$sde_i = "sdeimport -o create -l $newlayer,shape -f - -s $t_serv -i $t_inst -u $t_user -p $t_pass -k $t_user\_defaults -M $opt{M}";
	
		&ExecCmd("$sde_x | $sde_i");
		&alterReg("$newlayer","$opt{M}");

	} elsif ("$arg" eq "truncate_append") {
		&truncateLayer("$newlayer");
	} 
	
	$sde_x = "sdeexport -o create -l $layer,shape -f - -s $f_serv -i $f_inst -u $f_user -p $f_pass ";
	$sde_i = "sdeimport -o append -l $newlayer,shape -f - -s $t_serv -i $t_inst -u $t_user -p $t_pass";
	&ExecCmd("$sde_x | $sde_i");
	&updateMetadata("$newlayer");
}

sub isDone {
	my($layer,$arg) = @_;
	my $isDone="FALSE";
	
	if ("$arg" eq "grant") { 
		my $sql = qq{ select grantee from user_tab_privs where table_name = upper('$layer') and grantee = 'ADOL' };
		my @ans = &readQuerySingle("$sql");
		if ("$ans[0]" =~ /adol/i) { $isDone = "TRUE"; }
	}
	if ("$arg" eq "analyze") {
		my $sql = qq{ select num_rows from user_tables where table_name = upper('$layer') };
		my $features = &readQuerySingle("$sql");
		if ("$features" ne "") { $isDone = "TRUE"; }
	}
	if ("$arg" eq "register") {
		my $sql = qq{ select max(objectid) from $layer };
		my @objid = &readQuerySingle("$sql");
		if ("$objid[0]" > 0) { $isDone = "TRUE"; }
	}
	
	return ($isDone);
}

sub shapeInfo {
	my $shpfile = "@_";
	my $outfile = "shapeInfo.tmp";
	my $shpcount;
	&ExecCmd("shpinfo -f $shpfile","$outfile");
	
	open(SHPINFO_TMPFILE,"$outfile") or &Error("fatal","Couldn't open $outfile");
	while (<SHPINFO_TMPFILE>) {
		if (/Number of Shapes:\s+(.*)/) { $shpcount = "$1"; $shpcount =~ s/(\s|\t)//g; }
	}
	close(SHPINFO_TMPFILE);
	unlink("$outfile");
	return ($shpcount);
}
	
	
sub shapeLoadSimple {
	my ($shpfile,$layer,$etc) = @_;
	my $attr;
	if (-e "$shpfile.att") { 
		$attr = "file=$shpfile.att";
	#	$attr =~ s/.shp//i; 
	} elsif (-e "$shpfile.none") {
		$attr = "none";
	} else {
		$attr = "all";
	}
	
	my $rv = &ExecCmd("shp2sde -o create -l $layer,shape $sde_connection -f $shpfile -a $attr -k $user\_defaults -r $shpfile\_reject $etc");
	&updateMetadata("$layer");  ## notification to metadata
	return $rv;
}
sub shapeAppendSimple {
	my ($shpfile,$layer) = @_;
	my $attr;
	if (-e "$shpfile.att") { 
		$attr = "file=$shpfile.att"; 
	#	$attr =~ s/.shp//i; 
	} elsif (-e "$shpfile.none") {
		$attr = "none";
	} else { 
		$attr = "all"; 
	}
	my $rv = &ExecCmd("shp2sde -o append -l $layer,shape $sde_connection -f $shpfile -a $attr -r $shpfile\_reject");
	
	&updateMetadata("$layer"); ## notification to metadata
	return $rv;
}

sub load_monitor {
	&sqlConnect("$opt{p}");
	
	my $sql = qq{ select count(0) from $opt{l} };
	my @rows = &readQuerySingle($sql);
	my $now = `date`; chop($now);
	#my $pct = ($rows[0]/$opt{g})*100;
	#print "$now\t$server.$user.$opt{l}\t$rows[0] of $opt{g} rows loaded.\t$pct\% done.\n";
	print "$now\t$server.$user.$opt{l}\t$rows[0] loaded.\n";
}


sub get_layers {
	my $profile = "@_";
	my ($serv,$inst,$user,$pass,$host,$tab,$dbtune) = &profile("$profile");
	my $where;
	$user =~ tr/[a-z]/[A-Z]/;
	&sqlConnect("$user/$pass\@$host");
	if ($user eq "adol") {
		$where = "";
	} else {
		$where = qq{ where owner = '$user' };
	}
	my $sql = qq{ select table_name from sde.layers $where };
	my @ans = &sql($sql);
	&sqlDisconnect;
	return(@ans);
}



#------------------------------------------------------------------------------------------------------#
					### RASTER ###
#------------------------------------------------------------------------------------------------------#

sub getRasterID {
}

sub dropPyramids {
	my($layer,$raster_id) = @_;
	my $rv = &ExecCmd("sderaster -o pyramid -l $layer,image -v $raster_id -L 0 $sde_connection");
	return $rv;
}

sub buildPyramids {
	my($layer,$raster_id,$interpol) = @_;
	if (!defined $interpol) { $interpol = "nearest"; }
	my $rv = &ExecCmd("sderaster -o pyramid -l $layer,image -v $raster_id -L -1 -I $interpol $sde_connection");
	return $rv;
}

sub dropColormap {
	my($layer,$raster_id) = @_;
	my $rv = &ExecCmd("sderaster -o colormap -l $layer,image -v $raster_id -d $sde_connection");
	return $rv;
}

sub addColormap {
	my($layer,$raster_id,$file) = @_;
	my $rv = &ExecCmd("sderaster -o colormap -l $layer,image -v $raster_id -f $file $sde_connection");
	return $rv;
}

sub buildStats {
	my($layer,$raster_id) = @_;
	my $rv = &ExecCmd("sderaster -o stats -l $layer,image -v $raster_id $sde_connection");
	return $rv;
}

sub mosaicRaster {
	my($layer,$raster_id,$file) = @_;
	my $rv = &ExecCmd("sderaster -o mosaic -l $layer,image -v $raster_id -N -f $file $sde_connection");
	return $rv;
}

sub deleteMosaic {
	my($layer,$raster_id) = @_;
	my $rv = &ExecCmd("sderaster -o delete -v $raster_id -l $layer,image $sde_connection");
	return $rv;
}

sub describeRaster {
	my $layer = shift;
	my $tmpfile = "$ENV{ISDT_TEMP}/desc_$layer.tmp";
	my ($att,$val,$rid);
	my %desc;
	&ExecCmd("sderaster -o list -V -l $layer,image $sde_connection","$tmpfile");
	open(DESCRASTER_TEMP,"$tmpfile") or &Error("Could not open file: $tmpfile");
	while (<DESCRASTER_TEMP>) {
		#print; 
		chop;
		if ((/: /) && (! /^Extent/)) {
			m/^\s*(.*)\b.*:\s+(.*)\s*$/;
			$att = "$1";$val = "$2";
			$att =~ tr/[A-Z]/[a-z]/;
			$att =~ s/ /_/g;
			if ("$att" eq "raster_id") { $rid = "$val"; }
			#print "$att = $val\n";
			$desc{"$rid"}{"$att"}="$val";
		}	
	}
	close(DESCRASTER_TEMP);
	unlink("$tmpfile");
	# @desc = ($raster_id,$raster_dim,$rastile_dim,$pixtype,$compression,$img_pyramid, $minx,$miny,$maxx,$maxy,$stats);
	# @desc = ($raster_id=0,$raster_dim=1,$rastile_dim=2,$pixtype=3,$compression=4,$img_pyramid=5, $minx=6,$miny=7,$maxx=8,$maxy=9,$stats=10);
	
	return %desc;
}

############################ END SDE COMMANDS #################################
###############################################################################

###############################################################################
############################# BEGIN FTP ACCESS ################################


sub ftpConnect {
	my ($ftp_site,$ftp_user,$ftp_pass) = @_;
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
	my $rv = $ftp_handle->cwd("$ftp_dir");
	$logger->info("Current FTP dir is $ftp_dir");
	return $rv;
}

sub ftpDirList {
	my $filter = shift || "";
	my @ftp_dirlist = $ftp_handle->ls("$filter") or &Error("email|fatal","Could not get FTP directory listing");
	return @ftp_dirlist;
}

sub ftpGet {
	my ($ftp_file,$local_file) = @_;
	$logger->info("$ftp_file --> $local_file");
	my $rv = $ftp_handle->get("$ftp_file","$local_file");
	return $rv;
}

sub ftpDisconnect {
	$ftp_handle->quit;
	$logger->info("FTP Connection Closed.");
}


############################ END FTP ACCESS ###################################
###############################################################################


###############################################################################
############################# BEGIN DATABASE ACCESS ###########################


sub dbfConnect {
	my $dir = shift;
	$dbf_handle = DBI->connect("DBI:XBase:$dir")
		or &Error(-1,"Couldn't connect: $DBI::errstr");
	return $dbf_handle;
}
sub dbfConnect2 {
	my $dir = shift;
	$dbh{"$dir"} = DBI->connect("DBI:XBase:$dir")
		or &Error(-1,"Couldn't connect: $DBI::errstr");
	$logger->info("DBF connect: $dir");
	return 0;
}

sub sqlConnect {
	my $handle = shift || "$opt{p}"; 
	undef($SQL_ERROR);
	&getConnectionInfo("$handle");
	if("$handle" =~ /system/i) {
		print "Enter password for system\@$server access: ";
		system("stty -echo");
		chomp($pass=<STDIN>);
		system("stty echo");
		print "\n";
	}
	$dbh{"$handle"} = DBI->connect("DBI:Oracle:$server","$user","$pass")
	    	or do { 
			$logger->error("Couldn't connect to database using:  $user/$pass\@$server : $DBI::errstr");
			$SQL_ERROR = "$DBI::errstr";
			return 1;
		};

	$logger->info("SQL connect: $handle");
	return 0;
}  


sub sqlDisconnect {
	my $handle = shift || "$opt{p}";
	# $sth->finish;
	$dbh{$handle}->disconnect;
	$logger->info("SQL disconnect: $handle");
}

sub sqlAutoCommit {
	# set to 1 for on, 0 for off
	my $ac = shift; my $handle = shift || "$opt{p}";
	$dbh{$handle}->{AutoCommit}=$ac;
	$logger->info("AutoCommit set to $ac.");
}

sub sqlCommit {
	my $handle = shift || "$opt{p}";
	$dbh{$handle}->commit;
}

sub readQuerySingle { 
	my $sql = shift; my $handle = shift;
	undef($SQL_ERROR);
	$LogType="SQL";
	$Caller = (caller(1))[3]; $Caller =~ s/^main:://;
	
	$ExecCmdCount++;
	&log;
	print "S $sql\n";
	if (("$opt{log}" eq "CSH") && ("$Caller" !~ /getConnectionInfo/)) { print "SQL> $sql\n"; return (1,2,3,4,5); }

	#$logger->info("$sql");
	my @result = $dbh{"$handle"}->selectrow_array($sql) 
		or do {
			$logger->error("Couldn't execute: $DBI::errstr");
			$SQL_ERROR = "$DBI::errstr";
			return undef;
		};
		print "S2 $sql\n";
	$LogType = "RESP";
#	$logger->info("-->\t @result"); 
	return(@result);
}   

sub readQueryMultiRow {
	my $sql = shift; my $handle = shift || "$opt{p}";
	my (@result,$sth);
	undef $SQL_ERROR;
	$Caller = (caller(1))[3]; $Caller =~ s/^main:://;
	$LogType="SQL";
	$ExecCmdCount++;
	if ("$opt{log}" eq "CSH") { print "SQL> $sql\n"; return 1; }
	$logger->info("$sql");

 	$sth = $dbh{"$handle"}->prepare($sql)
             or do {
		     $logger->error("Couldn't prepare: $DBI::errstr");
		     $SQL_ERROR = "$DBI::errstr";
		     return undef;
	     };
        $sth->execute
             or do {
		     $logger->error("Couldn't execute: $DBI::errstr");
		     $SQL_ERROR = "$DBI::errstr";
		     return undef;
	     };
             
        my $array_ref = $sth->fetchall_arrayref();
        my $x=0;
        foreach my $row (@$array_ref) { 
		if ("$x" < 5) { $LogType="RESP"; $logger->info("--->\t @$row"); $x++; }
		push (@result,"@$row");
         }

        return($array_ref);
}

sub print_row { 
	my ($sql,$handle) = @_;
	my $sth;
	if ("$#_" < 1) { $handle = "$opt{p}"; }
	print "$sql\n";
	
	$sth = $dbh{"$handle"}->prepare( $sql )
	  	or &Error(-1,"\n\ncouldnt prepare: \n$sth->errstr\n\n$sql\n");
	$sth->execute
	  	or &Error(-1,"\n\ncouldnt Execute: \n$sth->errstr\n\n$sql\n");
	
	while ( my @row = $sth->fetchrow_array ) { 
		foreach my $elem (@row) { 
			if (!defined $elem) { $elem = " "; }
			print "$elem\t";
		}
		print "\n";
	}
}   
 
sub writeQuery {  
  # &writeQuery("$sql","n_tele","$ph1","$ph2");
  	my ($sql,$handle,@ph_values);
	$sql = shift; 
	$handle = shift || "$opt{p}"; 
	@ph_values = @_; #placeholder values
	my ($rows,$actionword,$rv,$sth) = undef;
	$SQL_ERROR = undef;
	$LogType="SQL";
	$Caller = (caller(1))[3]; $Caller =~ s/^main:://;
	$ExecCmdCount++;
	if ("$opt{log}" eq "CSH") { print "SQL> $sql\n"; return 1; }
 	if ("$opt{sqloutput}" eq "ON") { $logger->info("$sql"); }
 	$sth = $dbh{"$handle"}->prepare($sql) 
 		or do {
 			$logger->error("Couldnt prepare SQL: $sql");
 			$logger->error("$DBI::errstr");
 			$SQL_ERROR = "$DBI::errstr";
 			return -1;
 		};
	$rv = $sth->execute(@ph_values)
	  	or do {
			$logger->error("Couldnt execute sql: $sql");
			$logger->error("$DBI::errstr");
			$SQL_ERROR = "$DBI::errstr";
			return -1;
		};
	if ("$rv" eq 1) { $rows = "row"; } else { $rows = "rows"; }
	if ("$rv" eq "0E0") { $rv = 0; }
	$actionword = "$sql"; 
	$actionword =~ m/^\s*(insert|updat|delet|creat|alter|analyz)/i; 
	$actionword = "$1"."ed.";
	if ("$actionword" =~ /created|altered|analyz/) { $rv = "All"; }
	if ("$opt{sqloutput}" eq "ON") { $LogType="RESP";$logger->info("$rv $rows $actionword"); }
	return $rv;
} 

sub writeQueryCached {  
  # &writeQuery("$sql","n_tele","$ph1","$ph2");
  	my ($sql,$handle,@ph_values);
	$sql = shift; 
	$handle = shift || "$opt{p}"; 
	@ph_values = @_; #placeholder values
	my ($rows,$actionword,$rv,$sth) = undef;
	$SQL_ERROR = undef;
	$LogType="SQL";
	$Caller = (caller(1))[3]; $Caller =~ s/^main:://;
	$ExecCmdCount++;
	if ("$opt{log}" eq "CSH") { print "SQL> $sql\n"; return 1; }
 	if ("$opt{sqloutput}" eq "ON") { $logger->info("$sql"); }
 	$sth = $dbh{"$handle"}->prepare_cached($sql) 
 		or do {
 			$logger->error("Couldnt prepare SQL: $sql");
 			$logger->error("$DBI::errstr");
 			$SQL_ERROR = "$DBI::errstr";
 			return -1;
 		};
	$rv = $sth->execute(@ph_values)
	  	or do {
			$logger->error("Couldnt execute sql: $sql");
			$logger->error("$DBI::errstr");
			$SQL_ERROR = "$DBI::errstr";
			return -1;
		};
	if ("$rv" eq 1) { $rows = "row"; } else { $rows = "rows"; }
	if ("$rv" eq "0E0") { $rv = -1; }
	$actionword = "$sql"; 
	$actionword =~ m/^\s*(insert|updat|delet|creat|alter|analyz)/i; 
	$actionword = "$1"."ed.";
	if ("$actionword" =~ /created|altered|analyz/) { $rv = "All"; }

	if ("$opt{sqloutput}" eq "ON") { $LogType="RESP";$logger->info("$rv $rows $actionword"); }
	return $rv;
} 



########################## END DATABASE ACCESS ############################### 
##############################################################################

##############################################################################
######################### BEGIN METADATA #####################################

sub updateMetadata {
	my($table) = shift;my $object = shift || "layers";
	my($sql,$sql2,$mdtable,$lu);
	$MSG1 = "Updating metadata ...   ";
	#&sqlConnect("is_metadata");
	$mdtable = "is_"."$server"."_"."$object"."_nv";
	$user  =~ tr/[a-z]/[A-Z]/;
	$table =~ tr/[a-z]/[A-Z]/;
	$sql = qq{ select LAST_UPDATED from $mdtable where owner = '$user' and object_name = '$table' };
	
	($lu)=&readQuerySingle($sql,"sdeadmin");
	if (defined $lu) {
		$sql2 = qq{ update $mdtable set last_updated = sysdate where owner = '$user' and object_name = '$table' };
	} else {
		$sql2 = qq{ insert into $mdtable (owner,object_name,last_updated) values ('$user','$table',sysdate) };
	}
	
	my $rv = &writeQuery($sql2,"sdeadmin");
	if ($rv > 0) { $MSG1 = "SUCCESS\n"; } else { $MSG1 = "** ERROR **\n$SQL_ERROR\n"; }
	&log;
}

################################# END METADATA #################################
################################################################################

1;

__END__

=pod     ################ DOCUMENTATION ###################

=head1 NAME

is_data_tools.pl - A Collection of PERL subroutines which are accessed by the IS Data Team's SDE Data scripts

=head1 SYNOPSIS

require "is_data_tools.pl";

=head2 Generic Tools

# Interprets -arg value pairs (-layer street), or -option (-q)
&ReadCommandLine('a:b:c');

#  ExecCmd will run a 'system' against the passed argument unless the program is run with -q (quiet)
&ExecCmd("command -option value > $file");

=head2 SDE Commands

=head2 Database Access

# Connect to an Oracle database.
&sqlConnect(<PROFILE_NAME>);   	

# Disconnect from an Oracle database.
&sqlDisconnect;	  # Disconnects from default profile (-p argument)
&sqlDisconnect(<PROFILE_NAME>);

# Performs INSERT, UPDATE, DELETE SQL statements.  Returns number of rows affected.
my $sql = qq{ insert into table foo (emp_id) values (1987) };
$rv = &writeQuery($sql);
$rv = &writeQuery($sql,<PROFILE_NAME>);			

# Perform SELECT SQL statements
my $sql = qq{ select table_name from table_list where user = 'Xena' };
&readQuerySingle($sql);

=head1 HISTORY

2002-Nov-12 	Version 1.0 

=head1 AUTHOR

David Bianco, dbianco@esri.com     ESRI, Internet Solutions Team

=cut






#############################################################__#############################################


1;

__END__
