#!/usr/bin/perl

# Scans a directory containing the output of the stream crawler and
# extracts the tweetids

$directory = shift or die "$0 [directory]";

for $f ( `ls $directory` ) {
    chomp($f);
    my $path = "$directory/$f";

    open(DATA, "gunzip -c $path | grep '{\"created_at\"' | ");
    while ( my $line = <DATA> ) {
	if ( $line =~ m/{"created_at":.*?,"id":(\d+),/ ) {
	    print "$1\n";
	}
    }
    close(DATA);
}
