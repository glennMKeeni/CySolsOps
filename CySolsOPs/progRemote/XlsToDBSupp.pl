#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Text::CSV;
use Spreadsheet::ParseExcel;
use utf8;
use Encode qw(encode decode);
my ($EncodingI, $EncodingO) = ("SJIS", "utf8");
my ($db_file, $dbh, $csv_dir, $supDir);
my ($s_insert_stmt);

my $csv = Text::CSV->new({ binary => 1, auto_diag => 1 });
my $LogF = "./XlsToDbSup.log";

&Init();
&DropSupportTable();
&CreateSupportTable();
&PopulateSupportTable();
&Terminate();

sub Init {
    $db_file = '../CySolsOPs.db';
    $dbh = DBI->connect("dbi:SQLite:dbname=$db_file", '', '', { RaiseError => 1, AutoCommit => 0, sqlite_unicode => 1 });
    $supDir = '../CySolsOpsToSoumu/CySolsOpsInv/CySolsOps/Support';
}

sub Terminate {
    $dbh->commit;
    $dbh->disconnect;
}

sub DropSupportTable {
    $dbh->do(q{
        DROP TABLE IF EXISTS support
    });
}

sub CreateSupportTable {
    $dbh->do(q{
        CREATE TABLE IF NOT EXISTS support (
            SupportFile TEXT,
            SupportSheet TEXT,
            Prod TEXT,
            Operation TEXT,
            Row INTEGER,
            Sequence TEXT,
            Blank TEXT,
            CstID TEXT,
            RegisteredPartner TEXT,
            CurrentPartner TEXT,
            CstName TEXT,
            CstDepartment TEXT,
            CstDesignation TEXT,
            ContactName TEXT,
            ProductCode TEXT,
            SerialNumber TEXT,
            Account TEXT,
            Password TEXT,
            LicenseCount INTEGER,
            DeliveredDate TEXT,
            StartUseDate TEXT,
            PermittedVersion TEXT,
            DeliveredVersion TEXT,
            SupportContactNumber TEXT,
            SupportStartdate TEXT,
            SupportEnddate TEXT,
            Comments TEXT
        )
    });
}

sub PopulateSupportTable {
    $s_insert_stmt = $dbh->prepare(q{
        INSERT INTO support VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    });

    my @support_files = (
        ["Support_NK4-WB0AX", "NK4-NANO-WB0AX"],
        ["Support_NK4-WB0AX", "NK4-SES(D)-WB0AX"],
    );

    for my $file (@support_files) {
        &populateSTableFromXls($file->[0], $file->[1]);
    }
}

sub populateSTableFromXls {
    my ($SupF, $SupS) = @_;
    my $SupFP = $supDir . "/" . $SupF . ".xls";

    unless (-f $SupFP) {
        warn "File not found: $SupFP";
        return;
    }

    my $parser = Spreadsheet::ParseExcel->new();
    my $workbook = $parser->parse($SupFP);
    unless (defined $workbook) {
        warn "Parsing error: " . $parser->error() . " in file $SupFP";
        return;
    }

    my $worksheet;
    for my $sheet ($workbook->worksheets()) {
        if ($sheet->get_name() eq $SupS) {
            $worksheet = $sheet;
            last;
        }
    }

    unless (defined $worksheet) {
        warn "Sheet $SupS not found in file $SupF";
        return;
    }

    my ($Col_min, $Col_max) = $worksheet->col_range();
    my ($Row_min, $Row_max) = $worksheet->row_range();
    my $BadC = "\x{ff0d}";
    my $BadD = "\x{fffd}"; # Unmappable characters

    for my $Row (5..$Row_max) {
        my @data = ($SupF, $SupS, 'PROD', 'OPERATION', $Row + 1);
        my $emptyrow = 1;

        for my $Col (0..$Col_max) {
            my $Cell = $worksheet->get_cell($Row, $Col);
            my $Val = defined $Cell ? $Cell->value() : "";
            $Val =~ s/$BadC//g if $Val;
            $Val =~ s/$BadD//g if $Val;
            $emptyrow = 0 if $emptyrow && $Val ne "";
            $Val = &validate_date($Val) if ($Col == 14 || $Col == 15 || $Col == 19 || $Col == 20);
            &Log(sprintf("Invalid date, SupF = %s, SupS = %s, Row = %d, Col = %d\n", $SupF, $SupS, ($Row + 1), $Col)) unless (defined $Val);
            push(@data, $Val);
        }

        next if $emptyrow;

        if (@data > 26) {
            $csv->combine(splice(@data, 26));
            my $last_values = $csv->string();
            push(@data, $last_values);
        }
        $s_insert_stmt->execute(@data);
        print "Inserted Row $Row\n";
    }
}

sub validate_date {
    my ($text) = @_;
    return "" unless $text;

    my @parts;
    if ($text =~ /\//) {
        @parts = split('/', $text);
    } elsif ($text =~ /-/) {
        @parts = split('-', $text);
    } else {
        return undef;
    }

    return undef if scalar @parts != 3;

    my $month = $parts[1];
    my ($year, $day);
    if (length($parts[0]) == 4) {
        $year = $parts[0];
        $day = $parts[2];
    } elsif (length($parts[2]) == 4) {
        $year = $parts[2];
        $day = $parts[0];
    } else {
        return undef;
    }

    return sprintf("%04d-%02d-%02d", $year, $month, $day);
}

sub Log
{
    my ($Msg) = @_;
    die "Could not open $LogF $!"
         unless open (LOGF, ">>$LogF");
    print LOGF $Msg;
    close LOGF;
}