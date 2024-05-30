#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Text::CSV;
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::SaveParser;
use Spreadsheet::WriteExcel;
use File::Copy qw(copy);
use FindBin;
use File::Spec;
use File::Path qw(make_path);
use POSIX qw(strftime);
use lib $FindBin::Bin;
require 'ComLib.pl';
require 'Helper.pl';
# use Data::Dumper;

my ($config, $dbh, $csv, $LogF, $outputDir);
my  $LogFN   = 'ExportSupportData.log';
my  $ConfF   = File::Spec->catfile($FindBin::Bin, '..', 'conf', 'CySolsOPs.conf');
    $config  = Helper::read_config($ConfF);

eval {
    init($config);
    Helper::Log($LogF, "Export Started");
    export_support_data($config, "Support_NK4-WB0AX", "NK4-NANO-WB0AX");
    export_support_data($config, "Support_NK4-WB0AX", "NK4-SES(D)-WB0AX");
    Terminate();
};
if ($@) {
    Helper::Log($LogF, "An error occurred: $@");
}

sub init {
    my $config = shift;
    make_path($config->{LOGDIR}) unless (-e $config->{LOGDIR});
    $LogF      = File::Spec->catfile($config->{LOGDIR}, $LogFN);
    $dbh       = DBI->connect("dbi:SQLite:dbname=$config->{DB}", '', '', { RaiseError => 1, AutoCommit => 1 });
    $csv       = Text::CSV->new({ binary => 1, auto_diag => 1 });
    $outputDir = $config->{EXPDIR} . "\\Support_" . ComLib::GetYMDhmsTS(time); 
}

sub export_support_data {
    my ($config, $SupportFile, $SupportSheet) = @_;
    my $templateDir = $config->{SPTDIR};
    make_path($outputDir);

    my $supportFilePath = "$outputDir/$SupportFile.xls";
    my $templateFile    = "$templateDir/$SupportFile.xls";
    unless (-e $templateFile) {
        Helper::Log($LogF, "\tTemplate file $templateFile not found!");
    }
    unless (-e $supportFilePath) {
        copy($templateFile, $supportFilePath) or die "Copy failed: $!";
    }

    my $sql = "SELECT * FROM support WHERE SupportFile = ? AND SupportSheet = ?";
    my $sth = $dbh->prepare($sql);
    $sth->execute("$SupportFile.xls", $SupportSheet);

    my $parser = Spreadsheet::ParseExcel::SaveParser->new();
    my $workbook = $parser->Parse($supportFilePath);
    my $worksheet = $workbook->worksheet($SupportSheet);
    my ($rec, $lastline) = (0, 5);
    while (my $row = $sth->fetchrow_arrayref) {
        my ($SupportFile, $SupportSheet, $Prod, $Operation, $Row, @columns) = @$row;
        my $col = 0;
        foreach my $data (@columns) {
            my $cell = $worksheet->get_cell($lastline, $col);
            $csv->parse($data);
            my @split_data = $csv->fields();
            foreach my $split_value (@split_data) {
                $worksheet->AddCell($lastline, $col++, $split_value);
            }
        }
        $rec++;
        $lastline++;
    }
    Helper::Log($LogF, "\tExported $rec support data to SupF: $SupportFile, SupS: $SupportSheet");
    $workbook->SaveAs($supportFilePath);
}

sub Terminate {
    $dbh->disconnect;
}