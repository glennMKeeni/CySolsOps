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
my  $LogFN   = 'ExportCustomerData.log';
my  $ConfF   = File::Spec->catfile($FindBin::Bin, '..', 'conf', 'CySolsOPs.conf');
    $config  = Helper::read_config($ConfF);

eval {
    init($config);
    export_customer_data($config);
};
if ($@) {
    Helper::Log($LogF, "An error occurred: $@");
}

sub init {
    my $config = shift;
    make_path($config->{LOGDIR}) unless (-e $config->{LOGDIR});
    $LogF = File::Spec->catfile($config->{LOGDIR}, $LogFN);
    $dbh  = DBI->connect("dbi:SQLite:dbname=$config->{DB}", '', '', { RaiseError => 1, AutoCommit => 1, sqlite_unicode => 1 });
    $csv  = Text::CSV->new({ binary => 1, auto_diag => 1 });
}

sub export_customer_data {
    my $config = shift;
    my $outputDir = $config->{EXPDIR};
    make_path($outputDir);

    my $sql = "SELECT * FROM customer";
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    my $outF = "$outputDir\\Customer_" . ComLib::GetYMDhmsTS(time) . ".xls";

    my $workbook  = Spreadsheet::WriteExcel->new("$outF");
    my $worksheet = $workbook->add_worksheet();

    my @headers = (
        '#', '顧客ＩＤ', '登録時代理店', '現販売代理店', '顧客名', '部署', '肩書', 
        '主担当者', '主担当者Ｅメール', 'TEL', 'FAX', '郵便番号', '住所', 
        '備考', '変更箇所', '業種'
    );

    my $col = 0;
    foreach my $header (@headers) {
        $worksheet->write(0, $col, decode('utf-8', $header));
        $col++;
    }

    my $row = 0;
    while (my @data = $sth->fetchrow_array) {
        $col = 0;
        foreach my $cell (@data) {
            $worksheet->write($row + 1, $col, $cell);
            $col++;
        }
        $row++;
    }
    $sth->finish();
    $dbh->disconnect();
    $workbook->close();
    Helper::Log($LogF, "Exported $row customer data to $outF");
}