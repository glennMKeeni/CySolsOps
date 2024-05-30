#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Spreadsheet::WriteExcel;
use FindBin;
use File::Spec;
use File::Path qw(make_path);
use POSIX qw(strftime);
use Encode qw(decode);
use Text::CSV;
use lib $FindBin::Bin;
require 'ComLib.pl';
require 'Helper.pl';
# use Data::Dumper;

my ($config, $dbh, $csv, $LogF);
my  $LogFN   = 'ExportProductData.log';
my  $ConfF   = File::Spec->catfile($FindBin::Bin, '..', 'conf', 'CySolsOPs.conf');
    $config  = Helper::read_config($ConfF);

eval {
    init($config);
    export_product_data($config);
};
if ($@) {
    Helper::Log($LogF, "An error occurred: $@");
}

sub init {
    my $config = shift;
    make_path($config->{LOGDIR}) unless (-e $config->{LOGDIR});
    $LogF = File::Spec->catfile($config->{LOGDIR}, $LogFN);
    $dbh  = DBI->connect("dbi:SQLite:dbname=$config->{DB}", '', '', { RaiseError => 1, AutoCommit => 1, sqlite_unicode => 1 });
    $csv  = Text::CSV->new({ binary => 1, auto_diag => 1, eol => $/ });
}

sub export_product_data {
    my $config = shift;
    my $outputDir = $config->{EXPDIR};
    make_path($outputDir);

    my $sql = "SELECT * FROM product";
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    my $outF = "$outputDir\\Product_" . ComLib::GetYMDhmsTS(time) . ".csv";

    open my $fh, "> :encoding(utf-8)", $outF or die "Could not open '$outF' $!\n";
    $csv->print($fh, ["ProdCode", "ProdName", "Model"]);
    my $count = 0;
    while (my $row = $sth->fetchrow_arrayref) {
        $csv->print($fh, $row);
        $count++;
    }
    $sth->finish();
    $dbh->disconnect();
    close $fh;
    Helper::Log($LogF, "Exported $count product data to $outF");
}