package Helper;
use strict;
use warnings;
use FindBin;
use File::Spec;

my $config_file = "";
$config_file = File::Spec->catfile($FindBin::Bin, '..', 'conf', 'CySolsOPs.conf');

sub read_config {
    $config_file = shift;
    open my $fh, '<', $config_file or die "Could not open '$config_file': $!";
    my %config;
    
    while (my $line = <$fh>) {
        chomp $line;
        next if $line =~ /^\s*$/;  # skip blank lines
        next if $line =~ /^\s*#/;  # skip comments
        if ($line =~ /^\s*(\w+)\s*=\s*(.+?)\s*$/) {
            $config{$1} = $2;
        }
    }
    close $fh;
    return \%config;
}

sub Log {
    my ($LogF, $Msg) = @_;
    my ($secs, $mins, $hrs, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
    my $TS = ComLib::GetLogTS(time);
    open my $LOGF, ">>:encoding(cp932)", $LogF;
    print $LOGF "$TS $Msg\n";
    close $LOGF;
}

1;