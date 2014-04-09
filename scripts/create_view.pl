#!/usr/local/bin/perl
require "is_data_tools_strict.pl";

&defineArgs("v","View name","Name of view to be created.");
&defineArgs("t","Table list","List of tables to be included in the view");
&defineArgs("c","Column list","List of columns to be included in the view");
&defineArgs("a","View column list","List of columns to be included in the view");
&defineArgs("w","Where clause","Where clause");
&defineArgs("i","","Interactive mode - All other options ignored");

my %opt = &ReadCommandLine('v:t:c:w:a:i');
&setOption("l","$opt{v}");

if (defined $opt{i}) {
    &setOption("log","SCREEN");
    print "\n\nView name> ";
    chomp($opt{v}=<STDIN>);
    &setOption("l","$opt{v}");
    print "\nTable list> ";
    chomp($opt{t}=<STDIN>);
    print "\nColumn list> ";
    chomp($opt{c}=<STDIN>);
    print "\nAlias list> ";
    chomp($opt{a}=<STDIN>);
    print "\nWhere clause> ";
    chomp($opt{w}=<STDIN>);
    print "\nProfile> ";
    chomp($opt{p}=<STDIN>);
    &setOption("p","$opt{p}");
    print "\nProcessing...\n\n";
}

&BeginProgram;

my ($serv,$inst,$user,$pass) = &getConnectionInfo;
my $string = qq{"([^"\\\\]*(?:\\\\.[^"\\\\]*)*)"}; # matches a string within double-quotes

$opt{t} =~ s/\"//g;
$opt{c} =~ s/\"//g;
$opt{a} =~ s/\"//g;
$opt{w} =~ s/\"//g;

&deleteTable("$opt{v}");
&createView("$opt{v}","$opt{t}","$opt{c}","$opt{a}","$opt{w}");
&grantADOL("$opt{v}");
&ExecCmd("sdetable -o describe -t $opt{v} -s $serv -i $inst -u $user -p $pass");

&EndProgram;
