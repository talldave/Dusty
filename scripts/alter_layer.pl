#!/usr/local/bin/perl -w
use strict;
# alter_layer.pl
# D.Bianco
require "is_data_tools_strict.pl";

defineArgs("g","","Grant Read-Only User Access");
defineArgs("u","method","Update statistics using sdetable -o update_dbms_stats -m COMPUTE|ESTIMATE");
defineArgs("ul","","Update statistics for large layers using a SQL procedure");
defineArgs("reg","","Alter registration (OBJECTID)");
defineArgs("index","column","Create an index based on the specified column");
defineArgs("rename","new table name","Rename layer to specified new name");
defineArgs("lg","area filter","Create a new layer with features larger than specified area");
defineArgs("ds","column","Create a new layer dissolved on specified column");
defineArgs("gr","tile size","Create a new layer grouped by given tile size");
defineArgs("grid","index grid size","Alter the grid size of the layer");
defineArgs("delete","","Delete layer");
defineArgs("S","Description_String","Adds descriptive text to the layer");
defineArgs("G","SRID","Alter projection reference");

my %opt = ReadCommandLine('file:gr:ds:lg:rename:index:reg:u:ul:g:delete:S:G:grid:pre:post');

&BeginProgram;
&getConnectionInfo;
my ($createappend,$newtable);

if (defined $opt{file}) {
    my $table;
	if (defined $opt{tl}) { 
		$createappend = "append"; 
		$newtable = "$opt{tl}"; 
	} else { 
		$createappend = "create"; 
		$newtable=""; 
	}
    open (IN,"$opt{file}") or &Error("fatal","Couldn't open $opt{file}");
    while (<IN>) {
        chop;
        $table = "$_";
        if ("$table" !~ /^\#/) {
            setOption("l","$table");
            alter("$table");
        }
    }
    close(IN);
} else {    
    alter("$opt{l}");
}

EndProgram();


sub alter {
    my $table = "@_";
    if (defined $opt{rename}) { 
        my $rename;
        if (defined $opt{pre}) { 
            $rename = "$opt{pre}"."_$table"; 
        } elsif (defined $opt{post}) {
            $rename = "$table"."_$opt{post}";
        } else {
            $rename = "$opt{rename}";
        }
    
        renameLayer("$table","$rename"); 
        $table="$rename"; 
        grantRO_USER("$table");
    }
    if (defined $opt{delete}) { deleteLayer("$table"); }
    if (defined $opt{gr})     { 
        #&deleteLayer("$table\_gr"); 
        groupLayer("$table","$opt{gr}","$createappend","$newtable"); 
        $table="$table\_gr"; 
    }
    if (defined $opt{ds})     { 
        deleteLayer("$table\_ds"); 
        dissolveLayer("$table","$opt{ds}"); 
        $table="$table\_ds";
    }
    if (defined $opt{lg})     { 
        deleteLayer("$table\_lg"); 
        largeLayer("$table","$opt{lg}"); 
        $table="$table\_lg";
    }
    if (defined $opt{index})  { indexLayer("$table","$opt{index}"); }
    if (defined $opt{grid})   { gridLayer("$table","$opt{grid}"); }
    if (defined $opt{reg})    { alterReg("$table","$opt{reg}"); }
    if (defined $opt{u})      { updateStats("$table","$opt{u}"); }
    if (defined $opt{ul})     { updateStatsLarge("$table"); }
    if (defined $opt{g})      { if ("$opt{g}" eq "1") { $opt{g} = "adol"; } grantRO_USER("$table","$opt{g}"); }
    if (defined $opt{S})      { descLayer("$table","$opt{S}"); }
    if (defined $opt{G})      { alterProj("$table","$opt{G}"); }
}
   

__END__


 
