#!/usr/bin/perl

# Scans a directory containing the output of the stream crawler and
# extracts the documents in the qrels


$directory = shift or die "$0 [directory] [qrels]";
$qrels = shift or die "$0 [directory] [qrels]";

open QRELS, $qrels or die;
my $keyRef;
while ( <QRELS> ) {
    chomp;
    my ($topic, $extra, $docid, $rel) = split(" ", $_);
    $keyRef->{$docid} = ();
}

for $f ( `ls $directory` ) {
    chomp($f);
    my $path = "$directory/$f";

    open(DATA, "tar xfO $path --wildcards '*.bz2' | pbzip2 -dc | grep '{\"created_at\"' | ");
    while ( my $line = <DATA> ) {
	if ( $line =~ m/{"created_at":.*,"id":(\d+),/ ) {
	    if (exists $keyRef->{$1}) {
	        print "$line";
	    }
	}
    }
    close(DATA);
}
