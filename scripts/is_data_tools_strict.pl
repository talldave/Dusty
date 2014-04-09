#!/usr/local/bin/perl -w
#
# is_data_tools_strict.pl - D.Bianco, ESRI
#
use strict;
use DBI;
use Log::Log4perl qw(get_logger :levels);
use Net::FTP;
use Mail::Mailer;
use File::Basename;
use Proc::Simple;

############################################
############ GLOBAL VARIABLES ##############

my $LOGHOME = "$ENV{LOGHOME}";
my $EXIT_VALUE = 0;
my $SQL_ERROR = undef;
$|=1;
my $ExecCmdCount=0;
my $ResultString = "[ ERROR ]";
my $CmdString = "";
my ($lastCmd_log);
my $Caller = "Begin";
my $LogType = "INFO";
my ($errLog,$errEmail,$errScreen);
$errLog=$errEmail=$errScreen=0;
my $pid = $$;
my $pwd = `pwd`; chop($pwd);
my $hostname = `hostname`; chop($hostname);
#my $boldon = `tput smso`; my $boldoff = `tput rmso`;
my (%opt,$parselist,@orgARGV);
my (%arg,%arg_short,$starttime,$starttime_e,$logstamp,$logfile,$logfile_new,$logid);
my ($elapsedtime,$last_stop,$totaltime,$cmd_id);
my (%dbh,$dbf_handle,$ftp_handle);
my ($logger,%logger);
my ($transCount,$SourceCount,$TotalCount,$TransferCount,$bg_process);
my ($Source,$Target);
my $prev_target = "";
$transCount=$TotalCount=$TransferCount=0;
my @lastcmdcontents;
my ($sde_connection,$server,$user,$pass,$inst,$database,$keyword);
my $script = basename($0);

############################################


sub ReadCommandLine {
        $parselist=$_[0] || "";
        $parselist="$parselist".":l:p:h:log:email";   ##  Global Command Line Arguments
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

        if (("$numArgs" == 0) or (defined $opt{h})) { &printUsage($parselist); }
        if (defined $opt{log}) { 
        $opt{log} =~ tr/[a-z]/[A-Z]/; 
        if ("$opt{log}" !~ /(SCREEN|NONE|CSH|CRON)/) {  die "\nPlease use   -log <SCREEN|NONE|CSH|CRON>\n"; }
    } else { 
        $opt{log} = "SCREEN"; 
    }
    if (!defined $opt{email}) { $opt{email} = "$ENV{ISDT_EMAIL}"; } else { $opt{email} = "$ENV{ISDT_EMAIL},$opt{email}"; }
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
    
    $arg{h}         = "Help";
#   $arg{q}         = "Quiet operation (only print statements, no execute).";
    $arg{l}         = "SDE Layer";
    $arg{p}         = "SDE connection profile.";
#   $arg{proj}      = "Project";
    $arg{file}      = "Filename of layers to loop through";
#   $arg{sfips}         = "State FIPS";
    $arg{pre}       = "Prefix to be given to layer name.";
    $arg{post}      = "Suffix given to new layer name";
    $arg{log}       = "Specify logfile option.  Output to logfile is default";
	$arg{email} 	= "Email logfile to someone in addition to \$ISDT_EMAIL";
    
    $arg_short{l}       = "layer";
    $arg_short{p}       = "profile";
    $arg_short{f}       = "filename";
#   $arg_short{proj}    = "project";
    $arg_short{file}    = "file";
#   $arg_short{sfips}   = "state_fips";
    $arg_short{pre}     = "prefix";
    $arg_short{post}    = "suffix";
    $arg_short{log}     = "screen|none|csh|cron";
	$arg_short{email} 	= "address";
    
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
    $lastCmd_log=shift || "$LOGHOME/isdt_$pid"."_$ExecCmdCount".".tmp";
    if ("$opt{log}" eq "CSH") { print "CMD: $cmd\n"; return 1; }
    $ExecCmdCount++;
    $LogType = "CMD";
    my $a=1;
	my $RETURN_VALUE;

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

    $Caller = (caller(1))[3]; $Caller =~ s/^main:://;
    if ("$Caller" eq "") { my @cmdarray = split(/\s+/,"$cmd"); $Caller = basename($cmdarray[0]); }
    #if("$opt{log}" eq "SCREEN") { print "\n"; }
    

    #print $boldon;
    $logger->info("$cmd");
	if (uc("$server.$user.$Target") ne uc("$server.$user.$prev_target")) { 
		my $old_cmdstring = "$CmdString";
		print "\n";
		$CmdString    = "** Working with ArcSDE layer $server.$user.$Target **";
		$ResultString = "";
    	$logger{"SCREEN_CMD"}->info;
    	$logger{"SCREEN_RES"}->info;
		$prev_target = "$Target";
		$CmdString = "$old_cmdstring";
	}

    if ("$CmdString" eq "") { $CmdString = "$cmd ......................................................................................."; }
    $logger{"SCREEN_CMD"}->info;
    $CmdString="";
    $ResultString = "[ ERROR ]";
    #print $boldoff;
    
    
    open(SAVEOUT, ">&STDOUT");
    open(SAVEERR, ">&STDERR");
    open(STDOUT,">>$lastCmd_log") or &Error("fatal","Can't redirect output to $lastCmd_log");
    open(STDERR, ">&STDOUT") || &Error(-1,"Can't duplicate stdout");
    select(STDERR); $| = 1;     # make unbuffered
    select(STDOUT); $| = 1;     # make unbuffered
    
	#if ($Caller =~ /export_import|shapeAppendSimple|shapeLoadSimple/) {
		#$bg_process = Proc::Simple->new(); 
		#$RETURN_VALUE = $bg_process->start("$cmd");
		##&countStatus;
	#} else {
    	$RETURN_VALUE = system ($cmd); 
	#}
    
    close(STDOUT); 
    close(STDERR);
    open(STDOUT, ">&SAVEOUT");
    open(STDERR, ">&SAVEERR");
    close(SAVEOUT);
    close(SAVEERR);
    
    #$logger->debug("return_value = $RETURN_VALUE");
    #my $elapsed_time = &chrono("elapsed");
    #if ("$RETURN_VALUE" == 0) { 
    #   $logger->info("\tSUCCESS $elapsed_time"); 
    #   &lastCmd;
    #} else { 
    #   $logger->error("\t*** ERROR *** $elapsed_time"); 
    #   &lastCmd;
    #}
    my $rv = &lastCmd;
#   &logSummary("$opt{l}","$server","$pid","$caller","$RETURN_VALUE","$elapsed_time","$cmd","ExecCmd");
    
    if ($rv == 0) { $ResultString = "[ ok   ]"; }
    #if (("$Caller" eq "deleteLayer") and ($rv == -37)) { $ResultString = "[ no_exist ]"; }
    #if ("$Caller" eq "shapeAppendSimple") {
 #       $TransferCount = &get_count("transfer_count");
#        $SourceCount = &get_count("source");
	#}    
    $logger{"SCREEN_RES"}->info;
    $ResultString = "[ ERROR ]";
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
    &initLogger($level);
    if ("$opt{log}" =~ /(SCREEN|DUMMY)/) {
        #print "\nUse: ( touch $ENV{HOME}/pause$pid ) to pause execution.\n";
        #print "Use: ( touch $ENV{HOME}/stop$pid ) to stop execution.\n";
        if ("$opt{log}" ne "NONE") { print "\nUse: ( logreader.pl $logstamp $pid ) to see detailed logfile.\n"; }
        print "\n";
    }
    #if (defined $opt{l}) { $temp_var = "$opt{l}"; } else { $temp_var = "dummy"; }
    if (!defined $opt{p}) { &setOption("p","#NO_PROFILE_SET"); }
    #&setOption("l","Profile: $opt{p}");
    
    if (!defined $opt{l}) { &setOption("l","#NO_LAYER"); }
    if (!defined $opt{file}) { $myfile = "#NO_FILE"; } else { $myfile = "$opt{file}"; }
    
    
    $logger->info("Command= $script @orgARGV \&"); 
    $logger->info("Server= $hostname");
    $logger->info("pwd= $pwd");
    $logger->info("log= logreader.pl $logstamp $pid");
    $logger->info("Profile= $opt{p}");
    $logger->info("Project= #UNDEFINED");
    $logger->info("File= $myfile");
    #&setOption("l","$temp_var"); 
    &MailMe("Script started....");

    
    my $SDE_ADMIN = 'sdeadmin/sdeadmin@ferrari';
    $dbh{sdeadmin} = DBI->connect("DBI:Oracle:","$SDE_ADMIN")
        or &Error("","Couldn't connect to database using:  $SDE_ADMIN : $DBI::errstr");
    $cmd_id = "$pid"."$logstamp";
    my $objname;

    if (defined $opt{file}) { $objname = "$opt{file}"; } else { $objname = "$opt{l}"; }
    my $sql = qq{ insert into is_log_command values ($cmd_id,$pid,null,'$script','@orgARGV',0,'$objname',sysdate,null,null,'$opt{p}','logreader.pl $logstamp $pid') };
    &writeQuery("$sql","sdeadmin");
    #&logSummary("$opt{l}","$opt{p}","$pid","BEGIN","0","00:00:00","$script","unix");

}

sub initLogger {   
    my $level = shift || "$INFO";
    my $logstamp = &chrono("start");
    #my ($layout,$screen_layout,$isdt);
    my ($layout,$screen_layout,$isdt,$screen_cmd_layout,$screen_res_layout);
    #my $file_appender; my $stdout_appender;
        $logger = get_logger("main_logger");
        $logger{"SCREEN"} = get_logger("screen_logger");
        $logger{"SCREEN_CMD"} = get_logger("screen_cmdlogger");
        $logger{"SCREEN_RES"} = get_logger("screen_reslogger");
        $logger{"LOG"} = get_logger("file_logger");
        $logger{"MAIN"} = get_logger("main_logger2");
        Log::Log4perl::Layout::PatternLayout::add_global_cspec('a', sub { return "$TransferCount" });
        Log::Log4perl::Layout::PatternLayout::add_global_cspec('b', sub { return "$TotalCount" });
        Log::Log4perl::Layout::PatternLayout::add_global_cspec('e', sub { return "$Caller" });
        Log::Log4perl::Layout::PatternLayout::add_global_cspec('f', sub { return "$LogType" });
        Log::Log4perl::Layout::PatternLayout::add_global_cspec('g', sub { return "$opt{l}" });
        Log::Log4perl::Layout::PatternLayout::add_global_cspec('i', sub { return "$ExecCmdCount" });
    Log::Log4perl::Layout::PatternLayout::add_global_cspec('j', sub { return "$ResultString" });
    Log::Log4perl::Layout::PatternLayout::add_global_cspec('k', sub { return "$CmdString" });
    Log::Log4perl::Layout::PatternLayout::add_global_cspec('o', sub { return "$server.$user.$Target" });

    #my $layout = Log::Log4perl::Layout::PatternLayout->new("[%d][%h %g %P %i] %m%n");
    #if ("$opt{log}" =~ /(NONE)/) {
    #   $layout = Log::Log4perl::Layout::PatternLayout->new(" ");
    #} else {
    #   $layout = Log::Log4perl::Layout::PatternLayout->new("[%d][%g %P %i] %m%n");
#$layout = Log::Log4perl::Layout::PatternLayout->new("[%d{yyyy-MMM-dd HH:mm:ss}][:%P:][%g %a:%b %e_%i %f] %m%n");
$layout = Log::Log4perl::Layout::PatternLayout->new("[%d][:%P:][%g %e_%i %f] %m%n");
#$screen_layout = Log::Log4perl::Layout::PatternLayout->new("[%d %f] %m%n");
#$screen_layout = Log::Log4perl::Layout::PatternLayout->new("[%d{ABSOLUTE} %f] %m%n");

#$screen_layout = Log::Log4perl::Layout::PatternLayout->new("[%d{HH:mm:ss} %f] %m%n");

$screen_cmd_layout = Log::Log4perl::Layout::PatternLayout->new("%d{HH:mm}- %-.80k");
$screen_res_layout = Log::Log4perl::Layout::PatternLayout->new(" %7j%n");
    #}
    
    if ("$opt{log}" =~ /CRON/) { $isdt = "cron_"; } else { $isdt = "isdt_"; }

    my $file_appender = Log::Log4perl::Appender->new(
    "Log::Log4perl::Appender::File",
    name      => "filelog",
    mode      => "append",
    filename  => "$LOGHOME/$isdt$logstamp.log");
    $file_appender->layout($layout);
    
    #my $stdout_appender =  Log::Log4perl::Appender->new(
    #"Log::Log4perl::Appender::Screen",
    #name      => "screenlog",
    #stderr    => 0);
    #$stdout_appender->layout($screen_layout);
    
    my $stdout_cmd_appender =  Log::Log4perl::Appender->new(
    "Log::Log4perl::Appender::Screen",
    name      => "screenlog_cmd",
    stderr    => 0);
    $stdout_cmd_appender->layout($screen_cmd_layout);
    
    my $stdout_res_appender =  Log::Log4perl::Appender->new(
    "Log::Log4perl::Appender::Screen",
    name      => "screenlog_res",
    stderr    => 0);
    $stdout_res_appender->layout($screen_res_layout);
    
    if ("$opt{log}" =~ /SCREEN/) { 
      ### Log to logfile and screen
        $logger->add_appender($file_appender);
        $logger{"MAIN"}->add_appender($file_appender); 
        #$logger->add_appender($stdout_appender);
        #$logger{"MAIN"}->add_appender($stdout_appender);
     
    } elsif ("$opt{log}" =~ /NONE/) { 
      ### Log to screen only
        #$logger->add_appender($stdout_appender);
        #$logger{"MAIN"}->add_appender($stdout_appender);  
      
    } else {
      ### Log to logfile only
        $logger->add_appender($file_appender);
        $logger{"MAIN"}->add_appender($file_appender);   
    }
      
    $logger{"LOG"}->add_appender($file_appender);
    #$logger{"SCREEN"}->add_appender($stdout_appender);
    $logger{"SCREEN_CMD"}->add_appender($stdout_cmd_appender);
    $logger{"SCREEN_RES"}->add_appender($stdout_res_appender);

    $logger->level($level);
    $logger{"MAIN"}->level($level);
    $logger{"LOG"}->level($level);
    #$logger{"SCREEN"}->level($level);
    $logger{"SCREEN_CMD"}->level($level);
    $logger{"SCREEN_RES"}->level($level);

    return $logger;
}
  
sub MailMe {
    if ("$opt{email}" !~ /OFF/i) {
        my($mesg) = @_;
        $mesg =~ s/\n/<br>\n/g;
        $mesg =~ s/stat=\(ERROR\)/<font color=\"red\" size=\"3\">stat=\(ERROR\)<\/font>/g;
        my $type = 'sendmail';
        my $ctype = "text/html";
        #my $type = 'mail'; # Can't locate Mail/Mailer/mail.pm
        
        my $email = new Mail::Mailer($type);
    #   my $email = new Mail::Mailer("smtp", Server=>"email-server");

        my $recp = "$opt{email}";
        
        my $layer;

        if (defined $opt{file}) { 
            $layer = "$opt{file}";
        } else {
            $layer = "$opt{l}";
        }
        $layer =~ tr/[a-z]/[A-Z]/;
        
        my $subj = "DST: $script $layer ($pid:$ExecCmdCount)";
        my %headers = (
                'To' => qq{$recp},
                'From' => qq{$hostname},
                'Subject' => qq{$subj},
                'Content-type' => qq{$ctype}
                   );

        $email->open(\%headers);
        
        print $email "<html>\n";
        print $email "\n$script @orgARGV<br><br>\n\n";
        print $email "PID is $pid<br>\n";
        print $email "Command count is $ExecCmdCount<br>\n";
        print $email "logreader.pl $logstamp $pid<br><br>\n\n-------------------<br><br>\n\n";
        print $email $mesg;
        print $email "\n\n</html>\n";

        $email->close();
        $logger->info("Email sent:   $subj");
    }
    return 1;
}

sub lastCmd {
    my ($notify) = shift || "MAIN";
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
		if (/error/i) {
			$rv = -99;
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

    $logger{$notify}->info("rv=($rv);stat=($success);fp=($TransferCount);tfp=($TotalCount);time=($elapsed_time);fps=($fps);");
   # $TransferCount=0;
    $LogType = "RESP";
    foreach my $line (@lastcmdcontents) { $logger{$notify}->info("$line"); }
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
    if ($seconds == 0) { $seconds = 1; }
    my $fps = $TotalCount / $seconds;
    $fps = sprintf("%.3f", $fps);
    $logger->info("Total Time= $totaltime");
    $logger->info("Total Features= $TotalCount");
    $logger->info("Total Features per Second= $fps");
    foreach my $profile (keys %dbh) { &sqlDisconnect($profile); }   
    foreach my $file (<$LOGHOME/isdt_$pid*>) { unlink("$file"); }
    $logger->info("Command= $script @orgARGV \&"); 

    
    
    if ("$opt{log}" eq "CRON") { $cmd = "logreader_cron.pl"; } else { $cmd = "logreader.pl"; }
    my $logfile = `$cmd $logstamp $pid`;
    &MailMe("End Of Script. Ran for $totaltime\n\n<LOGFILE>\n$logfile\n\n.\n");
    #&logSummary("$opt{l}","$opt{p}","$pid","--END","$EXIT_VALUE","$elapsed_time","$script","unix");

    print "cumplido ($EXIT_VALUE)\n\n";
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
    my $oldLogType = "$LogType";
    $LogType = "ERROR";
    
#   if (("$error_value" =~ /LOG/) || ("$errLog" == 1)) {
#       if (defined $error_message) { 
#           $logger{$error_value}->error("$error_message"); 
#       } else {
#           &lastCmd("log");
#       }
#   } 
    
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
        $CmdString = "$error_message";
        $logger{"SCREEN_CMD"}->error;
        $logger{"SCREEN_RES"}->error;
        $logger->error("ERROR:   $error_message");
        $msg = "$error_message";
    }
#   if (("$error_value" =~ /SCREEN/) || ("$errScreen" == 1)) {
#       if (defined $error_message) {
#           $logger{"$error_value"}->error("$error_message"); 
#       } else {
#           &lastCmd("screen");
#       }
    #}
    if (("$error_value" =~ /EMAIL/) || ("$errEmail" == 1)) { &MailMe($msg); }
    if ("$error_value" =~ /FATAL/) { &EndProgram; }
    $LogType = "$oldLogType";
    
    
}


################################# END GENERIC TOOLS ###########################
###############################################################################

###############################################################################
################################## BEGIN SDE COMMANDS #########################

sub getConnectionInfo {
    my $profile = "@_";
    my $kword;
    if ("$profile" eq "") { $profile = "$opt{p}"; }
    if (!defined $profile) { &Error(-1,"Can not retrieve SDE Connection information. No profile specified."); }
    $logger->level($ERROR);
    my $sql = qq{ select SERVER,INSTANCE,USERNAME,PASSWORD,DATABASE,KEYWORD from IS_USERS where PROFILE = lower('$profile') };
    my @row = &readQuerySingle($sql,"sdeadmin");
    if ("$#row" < 3) { &Error("fatal","Can not retrieve SDE Connection information. Invalid profile ($profile)"); }
    #($server,$inst,$user,$pass,$database,$kword) = split(/ /,"@row");
    $server   = "$row[0]"; $server = uc("$server");
    $inst     = "$row[1]"; 
    $user     = "$row[2]"; $user = uc("$user");
    $pass     = "$row[3]";
    $database = "$row[4]" if defined $row[4];
    $kword    = "$row[5]" if defined $row[5];
    
    if (defined $database) {
        $sde_connection = "-s $server -i $inst -u $user -p $pass -D $database";
    } else {
        $sde_connection = "-s $server -i $inst -u $user -p $pass";
    }
    if (defined $kword) {
        $keyword = "$kword";
    } else {
        $keyword = "$user\_defaults";
    }
    
    $logger->level($DEBUG);
    return(@row);
}

sub updateStats {
    $Target =shift; my $method = shift || "ESTIMATE";
    if ("$method" eq "1") { $method = "ESTIMATE"; }
    $CmdString = "Analyzing table ($method) ..................................................................................................";
    my $rv = &ExecCmd("sdetable -o update_dbms_stats -t $Target -m $method $sde_connection -N");
    return $rv;
    ## automatic update in metadata
    ## no need to notify
    ## datestamp can be found in dba_tables
}

sub updateStatsLarge {
    $Target = "@_";
    my $rv;
    &sqlConnect("$opt{p}");
    my $sql = qq{ select layer_id from sde.layers where table_name = upper('$Target') and owner = upper('$user') };
    my @layer_id = &readQuerySingle($sql);
    $CmdString = "Update statistics ...............................................................................................";
    $rv = &ExecCmd("sqlplus $user/$pass\@$server \@updateStatsLarge $user $Target");
    if ("$layer_id[0]") {
        $rv = &ExecCmd("sqlplus $user/$pass\@$server \@updateStatsLarge $user F$layer_id[0]");
        $rv = &ExecCmd("sqlplus $user/$pass\@$server \@updateStatsLarge $user S$layer_id[0]");
    }
    
    my $sql2 = qq{ select index_name from user_indexes where table_name = upper('$Target') };
    my $resultArray = &readQueryMultiRow($sql2);
    foreach my $row (@$resultArray) {
        my ($index) = @$row;
        $rv = &ExecCmd("sqlplus $user/$pass\@$server \@updateIndexStatsLarge $user $index");
    }

    return $rv;
    ## same notes as updateStats
}

sub deleteLayer {
    $Target = "@_";
	my $rv=0;
	my $sql = qq{ select table_name from user_tables where table_name = upper('$Target') };
	&sqlConnect("$opt{p}");
	my @table = &readQuerySingle("$sql");
	if (!defined $table[0]) { $table[0] = "Table Does Not Exist"; }
	if ($table[0] eq uc($Target)) { 
    	$CmdString = "Deleting $Target .........................................................................................";
    	$rv = &ExecCmd("sdetable -o delete -t $Target $sde_connection -N");
	}
    return $rv;
    ## not kept track of yet
}

sub deleteTable {
	$Target = "@_";
   	$CmdString = "Deleting $Target ...........................................................................................";
	my $rv = &ExecCmd("sdetable -o delete -t $Target $sde_connection -N");
	return $rv;
}

sub truncateLayer {
    $Target = "@_";
    $CmdString = "Truncating ....................................................................................................";
    my $rv = &ExecCmd("sdetable -o truncate -t $Target $sde_connection -N");
    return $rv;
    ## not kept track of yet
}

sub renameLayer {
    my ($layer,$new_layer) = @_;
	$Target="$new_layer";
    $CmdString = "Renaming $server.$user.$layer .........................................................................";
    my $rv = &ExecCmd("sdetable -o rename -t $layer -T $new_layer $sde_connection -N");
    #my $sql = qq{ update IS_DBA_METADATA set object_name = '$new_layer' 
    #       where object_name = '$layer' and owner = '$user' };
    #&writeQuery($sql);
    return $rv;
    ## not kept track of yet
}

sub indexLayer {
    my ($layer,$ind_column) = @_;
	$Target="$layer";
	my $ind_name = "$layer"."_$ind_column"."_idx";
	$ind_name =~ s/^(N|NEW)_//i;
    $CmdString="Creating index on $ind_column ................................................................................";
    my $rv = &ExecCmd("sdetable -o create_index -t $layer -n $ind_name -c $ind_column -k $keyword $sde_connection");
    #&ExecCmd("sqlplus $user/$pass\@$server \@updateIndexStatsLarge $user $layer\_$ind_column\_ix");
    &ExecCmd("sqlplus $user/$pass\@$server \@analyze_index $ind_name");
    
    return $rv;
    ## automatic update in metadata
    ## no need to notify
}

sub createTable {
    my ($dbf_file,$table) = @_;
	$Target="$table";
    $CmdString="Loading file $dbf_file .......................................................................................";
    my $rv = &ExecCmd("tbl2sde -o create -t $table -f $dbf_file $sde_connection -T dBASE -a all -k $keyword");
    &updateMetadata("$table","tables"); ## notification to metadata
    return $rv;
}

sub appendTable {
    my ($dbf_file,$table) = @_;
	$Target="$table";
    $CmdString="Loading file $dbf_file .......................................................................................";
    my $rv = &ExecCmd("tbl2sde -o append -t $table -f $dbf_file $sde_connection -T dBASE");
    &updateMetadata("$table","tables"); ## notification to metadata
    return $rv;
}

sub createView {
    my ($view_name,$table_list,$col_list,$alias_list,$where_list) = @_;
	$Target="$view_name";
    my ($alias,$where);
    if ("$alias_list" eq "") { $alias = ""; } else { $alias = qq{-a "$alias_list"}; }
    if ("$where_list" eq "") { $where = ""; } else { $where = qq{-w "$where_list"}; }
    my $cmd = qq{sdetable -o create_view -T $view_name -t "$table_list" -c "$col_list" $alias $where $sde_connection};
    $CmdString = "Creating view ..................................................................................................";
    my $rv = &ExecCmd($cmd);
    &updateMetadata("$view_name","views"); ## notification to metadata
    $cmd =~ s/\'/\'\'/g;
    my $sql = qq{update is_$server\_views_nv set command = '$cmd' 
            where upper(owner) = upper('$user') and upper(object_name) = upper('$view_name') };
    &writeQuery("$sql","sdeadmin");
    return $rv;
}

sub deleteView {
    my $rv = &deleteLayer("@_");
    return $rv;
    ## not kept track of yet
}

sub gridLayer {
    my ($layer,$grid) = @_;
	$Target="$layer";
    $CmdString="Alter grid size to $grid ..............................................................................................";
    my $rv = &ExecCmd("sdelayer -o alter -g $grid -l $layer,shape $sde_connection -N");
    $CmdString="Set to NORMAL_IO ..............................................................................................";
    &ExecCmd("sdelayer -o normal_io -l $layer,shape $sde_connection");
	&updateStats("$layer");
    return $rv;
    ## not kept track of yet
}

sub setLayerDesc {
    my ($layer,$desc) = @_;
	$Target="$layer";
    $CmdString="Setting layer description ..........................................................................................";
    my $rv = &ExecCmd("sdelayer -o alter -S \"$desc\" -l $layer,shape $sde_connection");
    #my $sql = qq{ update IS_DBA_METADATA set DBA_NOTES = DBA_NOTES || ':$desc' };
    #&writeQuery($sql);
    return $rv;
}


sub load_only_io {
    $Target = "@_";
    $CmdString="Alter io_mode to LOAD_ONLY_IO ........................................................................................";
    my $rv = &ExecCmd("sdelayer -o load_only_io -l $Target,shape $sde_connection");
    return $rv;
}

sub normal_io {
    $Target = "@_";
    $CmdString="Alter io_mode to NORMAL_IO ............................................................................................";
    my $rv = &ExecCmd("sdelayer -o normal_io -l $Target,shape $sde_connection");
    return $rv;
}


sub registerLayer {
}

sub unregisterLayer {
}

sub alterReg {
    my ($table,$min_id) = @_;
	$Target="$table";
    if (!defined $min_id) { $min_id = 1; }
    $CmdString="Alter OBJECTID registration min_id= $min_id .........................................................................";
    my $rv = &ExecCmd("sdetable -o alter_reg -t $table -c OBJECTID -C SDE -L off -H visible -M $min_id -N -V single $sde_connection");
    return $rv;
    ## ** NEED TO ADD OBJECTID CHECK TO METADATA TABLES (GDB_OBJECTCLASSES) **
}

sub alterRegNone {
    $Target = "@_";
    $CmdString="Removing OBJECTID registration ..........................................................................................";
    my $rv = &ExecCmd("sdetable -o alter_reg -t $Target -C NONE -N $sde_connection");
    return $rv;
    ## this function is depracated
}
sub grantADOL {
    $Target = shift;
    my $grant_user = shift || "ADOL";
    $CmdString = "Grant $grant_user access ..........................................................................................";
    my $rv = &ExecCmd("sdetable -o grant -t $Target -A SELECT -U $grant_user $sde_connection");
    return $rv;
    ## automatic update in metadata
    ## no need to notify
}

sub alterProj {
    my ($layer,$srid) = @_;
	$Target="$layer";
    $CmdString="Alter projection to $srid ...............................................................................................";
    my $rv = &ExecCmd("sdelayer -o alter -G $srid -l $layer,shape $sde_connection");
    return $rv;
}

sub groupLayer {
    my ($layer,$tilesize,$append,$newlayer);
    $layer = shift; $tilesize = shift; $append = shift || "create"; $newlayer = shift || "$layer\_gr";
	$Target="$newlayer";
    if (defined $opt{post}) { $newlayer = "$newlayer"."_$opt{post}"; } 
    $CmdString="Enhance (group) $server.$user.$layer    tilesize= $tilesize ........................................................................";
    #if ("$server" =~ /foo/i) { $keyword = "defaults"; }
    my $rv = &ExecCmd("sdegroup -o $append -S $layer,shape -T $newlayer,shape -t $tilesize -e ls+ -a none -k $keyword $sde_connection");
    &updateMetadata("$newlayer"); ## notification
    #&setLayerDesc("$layer\_gr","sdegroup -t $tilesize");
    return $rv;
}
#sub dissolveLayer {
#   my ($layer,$column) = @_;
#   my $rv = &ExecCmd("sdegroup -o create -S $layer,shape -T $layer\_ds,shape -t column=$column -e ls+ -c 1000 -a none -k $user\_defaults $sde_connection");
#   &updateMetadata("$layer\_ds"); ## notification
#   #&setLayerDesc("$layer\_ds","sdegroup -t column=$column");
#   return $rv;
#}
sub largeLayer {
    my ($layer,$size) = @_;
    my $newlayer="$layer\_lg";
	$Target="$newlayer";
    #if (defined "$opt{post}") { $newlayer = "$newlayer"."_$opt{post}"; }
    $CmdString="Enhance (large) $server.$user.$layer      size= $size ............................................................................";
    my $rv = &ExecCmd("sdeexport -o create -l $layer,shape -a all -f - -w \"shape.area > $size\" $sde_connection | sdeimport -o create -l $newlayer,shape -f - $sde_connection -k $keyword");
    &updateMetadata("$newlayer");
    #&setLayerDesc("$layer\_lg","Large version of $layer: -w shape.area > $size");
    return $rv;
}
 
#sub updateLayerDate {
#use LWP::Simple;
#my $rv;
#my $layer = shift;
##my $handle = shift || "$opt{p}";
#if ("$server" =~ /foo1|foo2/i) {
#my $name = "$server".".$layer";
#$name = uc($name);
#my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
#$year += 1900;
#$mon++;
#my $date = sprintf("%04d-%02d-%02d",$year,$mon,$mday);
#my $url = "http://foo/lastmod.jsp?type=sdelayer&date=$date&names=$name";
#$logger->info("$url");
#my $content = get $url;
#}
#}


sub countStatus {
	my $poll = $bg_process->poll;
	my $sc_length = length($SourceCount);
	my $sleep = $sc_length*10;
	my $statlinelength = $sc_length*2+4+4;
	for(my $a=0;$a<=$statlinelength;$a++) { print "."; }
	while ($poll) {
		my $sql = qq{ select count(0) from $Target };
		my @count = &readQuerySingle("$sql");
		my $pct = $count[0] / $SourceCount;
		$pct = sprintf("%.1f", $pct);
		for(my $a=0;$a<=$statlinelength;$a++) { print "\b"; }
		print "$count[0]/$SourceCount ($pct%)";
		$poll = $bg_process->poll;
		sleep($sleep);
	}
}

sub getCount {
    my $layer  = shift; 
    my $handle = shift || "$opt{p}";
    if ("$layer" =~ /transfer_count/i) { return $TransferCount; }
    #&sqlConnect("$handle");
    my $sql = qq{ select count(0) from $layer };
    my @features = &readQuerySingle("$sql","$handle");
	if ("$features[0]" eq "0E0") { return "Not Found"; }
    return $features[0];
}

sub getLayerCount {
    my $layer = shift;
    my $handle = shift || "$opt{p}";
    my $tmpfile = "getLayerCount.tmp";
    my $layercount;
    &getConnectionInfo("$handle");
	$CmdString = "Retrieving source feature count .......................................................................................";
    my $rv = &ExecCmd("sdelayer -o stats -l $layer,shape $sde_connection","$tmpfile");
    open(LAYERCOUNT_TMP,"$tmpfile") or &Error("Couldn't open file: $tmpfile");
    while (<LAYERCOUNT_TMP>) {
        chop;
        if (/^Total Features:\s+(.*)/) { $layercount = "$1"; $layercount =~ s/ //g; }
    }
    close(LAYERCOUNT_TMP);
    unlink("$tmpfile");
    return $layercount;
}
 


sub describeAsADOL {
	my $layer = shift;
	$CmdString = "Describing layer as ADOL ........................................................................................";
	my $cmd = qq{ sdetable -o describe -t $user.$layer -s $server -u adol -p adol };
	my $rv =&ExecCmd("$cmd");
	return $rv;
}

sub describeLayer {
    my $layer = shift;
    my $tmpfile = "$ENV{ISDT_TEMP}/desc_$layer.tmp";
    my ($att,$val);
    my %desc;
	$Target="";
	$CmdString = "Getting description for $server.$user.$layer ...........................................................................";
	#$sde_connection =~ s/-i (.*) /-i sde:oracle9i /;
	#$sde_connection =~ s/-p (.*) /-p $1\@$server /;
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
    $CmdString="Exporting $server.$user.$layer ............................................................................................";
    if ("$export_type" eq "SDX") {
		$Target="$layer.sdx";
        $rv = &ExecCmd("sdeexport -o create -l $layer,shape -a all -f $layer.sdx $sde_connection");
    } elsif ("$export_type" eq "SHP") {
        &ExecCmd("sdelayer -o describe_long -l $layer,shape $sde_connection > $tmpfile");
        open(ENTITY_TMP,"$tmpfile") or &Error("Couldnt open $tmpfile");
        while (<ENTITY_TMP>) {
            if (/^Entities .............: (.*)$/) { $etype = "$1"; }
        }
        close(ENTITY_TMP);
        #unlink("$tmpfile");
        
        if ("$etype" =~ /(l|s)/) { $file_type = "arc";     }
        if ("$etype" =~ /a/)     { $file_type = "polygon"; }
        if ("$etype" =~ /p/)     { $file_type = "point";   }

		$Target="$layer.shp";
        $rv = &ExecCmd("sde2shp -o init -l $layer,shape -a all -f $layer -t $file_type $sde_connection");
    }
    return $rv;
}

sub newTemplate {
	my $layer = shift;
	my $newlayer = shift || "N_"."$layer";
	if (!defined $opt{M}) { &setOption("M","1"); }
	my $att="all";
	if (-e "$newlayer.att") { $att = "file=$newlayer.att"; }
	if (defined $opt{att})  { $att = "file=$opt{att}"; }

	#&deleteLayer("$newlayer");
	my $sde_x = "sdeexport -o create -l $layer,shape -f - $sde_connection -a $att -w \"$layer.shape < 0\"";
	my $sde_i = "sdeimport -o create -l $newlayer,shape -f - $sde_connection -k $keyword -M $opt{M}";
   
   $CmdString="Creating empty layer $newlayer from $layer .......................................................................";
   my $rv = &ExecCmd("$sde_x | $sde_i");
   my $objid = &readQuerySingle("select rowid_column from sde.table_registry where table_name = upper('$layer')");
   if (!defined $objid) { &alterReg("$newlayer","$opt{M}"); }

   if (("$opt{M}" > 1) && (defined $objid)) { &alterReg("$newlayer","$opt{M}"); }

	return $rv;
}

sub importLayer {
    my ($sdx_file,$layer) = @_;
#   my $sde_layer = "$layer"; $sde_layer =~ s/\.sdx//i;
    if (!defined $opt{M}) { &setOption("M","1"); }
    $CmdString="Importing $sdx_file ............................................................................................";
    my $rv = &ExecCmd("sdeimport -o create -l $layer,shape -f $sdx_file -M $opt{M} -k $keyword $sde_connection ");
    &updateMetadata("$layer");
    return $rv;
}

sub importAppendLayer {
    my ($sdx_file,$layer) = @_;
#   my $sde_layer = "$layer"; $sde_layer =~ s/\.sdx//i;
    if (!defined $opt{M}) { &setOption("M","1"); }
    $CmdString="Importing $sdx_file ............................................................................................";
    my $rv = &ExecCmd("sdeimport -o append -l $layer,shape -f $sdx_file -M $opt{M} $sde_connection");
    &updateMetadata("$layer");
    return $rv;
}


sub export_import {
    my ($src,$tgt,$arg,$layer,$newlayer) = @_;
    $arg =~ tr/[A-Z]/[a-z]/;

	#### CHECK FOR XY LAYER COMPATIBILITY  #  SCHEMA MATCHES # PROJECTION #####

    my ($sde_x,$sde_i,$att);
    $newlayer = $layer unless defined $newlayer;
	$Target="$newlayer";
    my ($f_serv,$f_inst,$f_user,$f_pass) = &getConnectionInfo("$src");
    my ($t_serv,$t_inst,$t_user,$t_pass) = &getConnectionInfo("$tgt");
    if (!defined $opt{M}) { &setOption("M","1"); }
	$att="all";
	if (-e "$newlayer.att") { $att = "file=$newlayer.att"; }
	if (defined $opt{att})  { $att = "file=$opt{att}"; }

    if ("$arg" eq "create") {
        &deleteLayer("$newlayer");
        $sde_x = "sdeexport -o create -l $layer,shape -f - -s $f_serv -i $f_inst -u $f_user -p $f_pass -a $att -w \"$layer.shape < 0\"";
        $sde_i = "sdeimport -o create -l $newlayer,shape -f - -s $t_serv -i $t_inst -u $t_user -p $t_pass -k $keyword -M $opt{M}";
    
		$CmdString="Creating empty layer ..............................................................................";
        &ExecCmd("$sde_x | $sde_i");
		my $objid = &readQuerySingle("select rowid_column from sde.table_registry where table_name = upper('$layer')");
		if (!defined $objid) { 
			&alterReg("$newlayer"); 
			if ("$opt{M}" != 1) { &alterReg("$newlayer","$opt{M}"); }
		}
				
        if (("$opt{M}" > 1) && (defined $objid)) { &alterReg("$newlayer","$opt{M}"); }
		if (defined $opt{empty}) { return 1; }

    } elsif ("$arg" eq "init") {
        &truncateLayer("$newlayer");
    } 

	&load_only_io("$newlayer");
    
    $sde_x = "sdeexport -o create -l $layer,shape -f - -s $f_serv -i $f_inst -u $f_user -p $f_pass ";
    $sde_i = "sdeimport -o append -l $newlayer,shape -f - -s $t_serv -i $t_inst -u $t_user -p $t_pass";
	$CmdString="Loading $f_serv.$f_user.$layer ...............................................................................";
    &ExecCmd("$sde_x | $sde_i");
	&normal_io("$newlayer");
    &updateMetadata("$newlayer");
}
sub export_import2 {
    my ($src,$tgt,$arg,$layer,$newlayer) = @_;
    $arg =~ tr/[A-Z]/[a-z]/;

	#### CHECK FOR XY LAYER COMPATIBILITY  #  SCHEMA MATCHES # PROJECTION #####

    my ($sde_x,$sde_i,$att);
    $newlayer = $layer unless defined $newlayer;
	$Target="$newlayer";
    my ($f_serv,$f_inst,$f_user,$f_pass) = &getConnectionInfo("$src");
    my ($t_serv,$t_inst,$t_user,$t_pass) = &getConnectionInfo("$tgt");
    if (!defined $opt{M}) { &setOption("M","1"); }
	$att="all";
	if (-e "$newlayer.att") { $att = "file=$newlayer.att"; }
	if (defined $opt{att})  { $att = "file=$opt{att}"; }

    if ("$arg" eq "create") {
        &deleteLayer("$newlayer");
        #$sde_x = "sdeexport -o create -l $layer,shape -f - -s $f_serv -i $f_inst -u $f_user -p $f_pass -a $att -w \"$layer.shape < 0\"";
        #$sde_i = "sdeimport -o create -l $newlayer,shape -f - -s $t_serv -i $t_inst -u $t_user -p $t_pass -k $keyword -M $opt{M}";
    
		#$CmdString="Creating empty layer ..............................................................................";
        #&ExecCmd("$sde_x | $sde_i");
		#my $objid = &readQuerySingle("select rowid_column from sde.table_registry where table_name = upper('$layer')");
		#if (!defined $objid) { &alterReg("$newlayer","$opt{M}"); }
				
        #if (("$opt{M}" > 1) && (defined $objid)) { &alterReg("$newlayer","$opt{M}"); }

    } elsif ("$arg" eq "init") {
        &truncateLayer("$newlayer");
    } 

	#&load_only_io("$newlayer");
    
    $sde_x = "sdeexport -o create -l $layer,shape -f - -s $f_serv -i $f_inst -u $f_user -p $f_pass ";
    $sde_i = "sdeimport -o create -l $newlayer,shape -f - -s $t_serv -i $t_inst -u $t_user -p $t_pass -k $keyword";
	$CmdString="Loading $f_serv.$f_user.$layer ...............................................................................";
    &ExecCmd("$sde_x | $sde_i");
	#&normal_io("$newlayer");
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
	$Target="$layer";
    my $attr;
    if (-e "$shpfile.att") { 
        $attr = "file=$shpfile.att";
    #   $attr =~ s/.shp//i; 
    } elsif (-e "$shpfile.none") {
        $attr = "none";
    } else {
        $attr = "all";
    }
	my $reject_shapefile = "$shpfile";
	$reject_shapefile =~ s/.shp$//;   $reject_shapefile .= "_reject.shp";
    $CmdString="Loading shapefile $shpfile .........................................................................................";
    #my $rv = &ExecCmd("shp2sde -o create -l $layer,shape $sde_connection -f $shpfile -a $attr -k $keyword -r reject.shp $etc");
    my $rv = &ExecCmd("shp2sde -o create -l $layer,shape $sde_connection -f $shpfile -a $attr -k $keyword -r $reject_shapefile $etc");
    &updateMetadata("$layer");  ## notification to metadata
    return $rv;
}
sub shapeAppendSimple {
    my ($shpfile,$layer) = @_;
	$Target="$layer";
    my $attr;
    if (-e "$shpfile.att") { 
        $attr = "file=$shpfile.att"; 
    #   $attr =~ s/.shp//i; 
    } elsif (-e "$shpfile.none") {
        $attr = "none";
    } else { 
        $attr = "all"; 
    }
	my $reject_shapefile = "$shpfile";
	$reject_shapefile =~ s/.shp$//;   $reject_shapefile .= "_reject.shp";
    $CmdString = "Appending shapefile $shpfile ...........................................................................................";
    #my $rv = &ExecCmd("shp2sde -o append -l $layer,shape $sde_connection -f $shpfile -a $attr -r reject.shp");
    my $rv = &ExecCmd("shp2sde -o append -l $layer,shape $sde_connection -f $shpfile -a $attr -r $reject_shapefile");
    
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

sub copyRaster {
	my $layer = shift;
 	my $targetras = shift || "$layer";
    my $sraster = qq{-f "-l $targetras,image -i 5151 -s 206.16.219.181 -u ned -p ned"}; 
	my $rv = ExecCmd("sderaster -o copy -l $layer,image -g -G 4267 -k $keyword $sde_connection $sraster");
	return $rv;
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
    my $rv = &ExecCmd("sderaster -o delete -v $raster_id -l $layer,image -y $sde_connection");
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
    
    $LogType = "INFO";
    $logger->info("SQL connect: $handle");
    return 0;
}  


sub sqlDisconnect {
    my $handle = shift || "$opt{p}";
    # $sth->finish;
    $dbh{$handle}->disconnect;
    $LogType = "INFO";
    $logger->info("SQL disconnect: $handle");
}

sub sqlAutoCommit {
    # set to 1 for on, 0 for off
    my $ac = shift; my $handle = shift || "$opt{p}";
    $dbh{$handle}->{AutoCommit}=$ac;
    $LogType = "INFO";
    $logger->info("AutoCommit set to $ac.");
}

sub sqlCommit {
    my $handle = shift || "$opt{p}";
    $dbh{$handle}->commit;
}

sub readQuerySingle { 
    my $sql = shift; my $handle = shift || "$opt{p}";
    undef($SQL_ERROR);
    $LogType="SQL";
    $Caller = (caller(1))[3]; $Caller =~ s/^main:://;

    $ExecCmdCount++;
    if (("$opt{log}" eq "CSH") && ("$Caller" !~ /getConnectionInfo/)) { print "SQL> $sql\n"; return (1,2,3,4,5); }

    $logger->info("$sql");
    my @result = $dbh{"$handle"}->selectrow_array($sql); 
    #die "-- @result -- NULL,NULL,NULL,NULL,NULL\n";
    # selectrow_array returns undef if error, which is same as if the selected content is NULL.  tricky to catch, so ignoring for now

    #   or do {
    #       $logger->error("Couldn't execute: $DBI::errstr");
    #       $SQL_ERROR = "$DBI::errstr";
    #       return undef;
    #   };
    $LogType = "RESP";
    my @goodresult;
    for my $x ( @result) { 
        push @goodresult, $x if $x; 
        # print "this is $x\n" if $x; 
    }
    $logger->info("-->\t @goodresult");
    # a NULL value returned for any of the @result elements returns warning: Use of uninitialized value in join or string...
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
    $actionword =~ m/^\s*(insert|updat|delet|creat|alter|analyz|drop|grant)/i; 
    $actionword = "$1"."ed."; $actionword =~ tr/[A-Z]/[a-z]/;  
    if ("$actionword" eq "droped") { $actionword = "dropped"; }
    if ("$actionword" =~ /created|altered|analyz|drop|grant/) { $rv = "All"; }
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
    if ("$rv" eq "0E0") { $rv = 0; }
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
    
    &writeQuery($sql2,"sdeadmin");
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
&sqlDisconnect;   # Disconnects from default profile (-p argument)
&sqlDisconnect(<PROFILE_NAME>);

# Performs INSERT, UPDATE, DELETE SQL statements.  Returns number of rows affected.
my $sql = qq{ insert into table foo (emp_id) values (1987) };
$rv = &writeQuery($sql);
$rv = &writeQuery($sql,<PROFILE_NAME>);         

# Perform SELECT SQL statements
my $sql = qq{ select table_name from table_list where user = 'Xena' };
&readQuerySingle($sql);

=head1 HISTORY

2002-Nov-12     Version 1.0 

=head1 AUTHOR

David Bianco, dbianco@esri.com     ESRI, Internet Solutions Team

=cut

