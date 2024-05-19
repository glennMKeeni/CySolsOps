#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Text::CSV;
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::SaveParser;
use Spreadsheet::WriteExcel;
use File::Copy qw(copy);
use File::Path qw(make_path);
# use Data::Dumper;

my ($dbFile, $templateDir, $outputDir, $dbh, $csv);

my $datestring = localtime();
print $datestring. "\n";

&Init();
generateXlsFromSupportTable("Support_NK4-WB0AX", "NK4-NANO-WB0AX");
generateXlsFromSupportTable("Support_NK4-WB0AX", "NK4-SES(D)-WB0AX");
&Terminate();

$datestring = localtime();
print $datestring;

sub Init {
    $dbFile       = '../CySolsOPs.db';
    $templateDir  = "../CySolsOpsToSoumu/CySolsOpsInv/CySolsOps/support_template";
    $outputDir    = "../ExcelsO";
    $dbh          = DBI->connect("dbi:SQLite:dbname=$dbFile", '', '', { RaiseError => 1, AutoCommit => 1 });
    $csv          = Text::CSV->new({ binary => 1, auto_diag => 1 });
}

sub generateXlsFromSupportTable {
    my ($SupportFile, $SupportSheet) = @_;
    make_path($outputDir);
    my $supportFilePath = "$outputDir/$SupportFile.xls";
    my $templateFile    = "$templateDir/$SupportFile.xls";
    unless (-e $templateFile) {
        die "Template file '$templateFile' not found!";
    }
    unless (-e $supportFilePath) {
        copy($templateFile, $supportFilePath) or die "Copy failed: $!";
    }

    my $sql = "SELECT * FROM support WHERE SupportFile = ? AND SupportSheet = ?";
    my $sth = $dbh->prepare($sql);
    $sth->execute($SupportFile, $SupportSheet);

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
    print sprintf("%5d Data has been successfully exported to Excel file: $SupportFile, Sheet: $SupportSheet\n", $rec);
    $workbook->SaveAs($supportFilePath);
}

sub Terminate {
    $dbh->disconnect;
}