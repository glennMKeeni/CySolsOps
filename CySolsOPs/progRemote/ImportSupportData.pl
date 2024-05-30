#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Text::CSV;
use Spreadsheet::ParseExcel;
use utf8;
use Encode qw(encode);
use FindBin;
use File::Spec;
use File::Path qw(make_path);
use File::Find;
use File::Basename;
use lib "$FindBin::Bin";
require 'ComLib.pl';
require 'Helper.pl';

my ($config, $dbh, $csv, $LogF, $SuppFs);
my  $LogFN   = 'ImportSupportData.log';
my  $ConfF   = File::Spec->catfile($FindBin::Bin, '..', 'conf', 'CySolsOPs.conf');
    $config  = Helper::read_config($ConfF);

    # support_files => [
    #     ["Support_NK4-WB0AX", "NK4-NANO-WB0AX"],
    #     ["Support_NK4-WB0AX", "NK4-SES(D)-WB0AX"]
    # ]

eval {
    &init($config);
    &drop_support_table();
    &create_support_table();
    $SuppFs = &get_support_files($config);
    &populate_support_table($config, @$SuppFs);
    &terminate($config);
    1;
} or do {
    my $error = $@ || 'Unknown error';
    &Helper::Log($LogF, "Error: $error");
};

sub init {
    my ($config) = shift;
    make_path($config->{LOGDIR}) unless -e $config->{LOGDIR};
    $LogF = File::Spec->catfile($config->{LOGDIR}, $LogFN);
    $dbh = DBI->connect("dbi:SQLite:dbname=$config->{DB}", '', '', { RaiseError => 1, AutoCommit => 0, sqlite_unicode => 1 });
    $csv = Text::CSV->new({ binary => 1, auto_diag => 1 });
}

sub terminate {
    $dbh->commit;
    $dbh->disconnect;
}

sub drop_support_table {
    $dbh->do(q{
        DROP TABLE IF EXISTS support
    });
    $dbh->commit;
}

sub create_support_table {
    $dbh->do(q{
        CREATE TABLE IF NOT EXISTS support (
            SupportFile TEXT NOT NULL,
            SupportSheet TEXT NOT NULL,
            Prod TEXT,
            Operation TEXT,
            Row INTEGER NOT NULL,
            Sequence TEXT NOT NULL,
            Blank TEXT,
            CstID TEXT NOT NULL,
            RegisteredPartner TEXT,
            CurrentPartner TEXT,
            CstName TEXT,
            CstDepartment TEXT,
            CstDesignation TEXT,
            ContactName TEXT,
            ProductCode TEXT NOT NULL,
            SerialNumber TEXT NOT NULL,
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

sub get_insert_statement {
    my $insert_stmt = $dbh->prepare(q{
        INSERT INTO support VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    });
    return $insert_stmt;
}

sub get_support_files {
    my $config = shift;
    my $supdir = $config->{SUPDIR};
    my @files;

    if (-d $supdir) {
        find(sub {
            if (/\.xls$/ && -f $File::Find::name) {
                my $file = $File::Find::name;
                my $parser = Spreadsheet::ParseExcel->new();
                my $workbook = $parser->parse($file);
                
                if (!defined $workbook) {
                    &Helper::Log($LogF, "Failed to parse $file: " . $parser->error());
                    return;
                }

                my @sheet_names;
                for my $worksheet ($workbook->worksheets()) {
                    if (is_sheet_empty($worksheet)) {
                        next;
                    }
                    push @sheet_names, $worksheet->get_name();
                }
                push @files, [$file, @sheet_names] if @sheet_names;
            }
        }, $supdir);
    } else {
        &Helper::Log($LogF, "Directory $supdir does not exist\n");
    }
    return \@files;
}

sub is_sheet_empty {
    my ($worksheet) = @_;
    my ($min_row, $max_row) = $worksheet->row_range();
    my ($min_col, $max_col) = $worksheet->col_range();
    for my $row ($min_row .. $max_row) {
        for my $col ($min_col .. $max_col) {
            my $cell = $worksheet->get_cell($row, $col);
            if (defined $cell && $cell->value() ne '') {
                return 0;  # Not empty
            }
        }
    }
    return 1;  # Empty
}

sub populate_support_table {
    my ($config, @files) = @_;

    my $SupFC = 0;
    for my $file_entry (@files) {
        my ($file_name, @sheet_names) = @$file_entry;

        for my $sheet_name (@sheet_names) {
            &populate_s_table_from_xls($config, $file_name, $sheet_name, get_insert_statement());
            $SupFC++;
        }
    }
    &Helper::Log($LogF, "Processed $SupFC Support files");
}

sub populate_s_table_from_xls {
    my ($config, $SupF, $SupS, $insert_stmt) = @_;
    # my $SupFP = File::Spec->catfile($config->{SUPDIR}, "$SupF.xls");
    &Helper::Log($LogF, "Processing $SupF");

    unless (-f $SupF) {
        &Helper::Log($LogF, "\tFile not found: $SupF");
        return;
    }

    my $parser = Spreadsheet::ParseExcel->new();
    my $workbook = $parser->parse($SupF);
    unless (defined $workbook) {
        &Helper::Log($LogF, "\tParsing error: " . $parser->error() . " in file $SupF");
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
        &Helper::Log($LogF, "\tSheet $SupS not found in file $SupF");
        return;
    }

    my ($Col_min, $Col_max) = $worksheet->col_range();
    my ($Row_min, $Row_max) = $worksheet->row_range();
    my $BadC = "\x{ff0d}";
    my $BadD = "\x{fffd}"; # Unmappable characters

    my ($succ, $fail, $Val) = (0, 0, "");
    for my $Row (5..$Row_max) {
        my @data = (basename($SupF), $SupS, 'PROD', 'OPERATION', $Row + 1);
        my $emptyrow = 1;

        for my $Col (0..$Col_max) {
            my $Cell = $worksheet->get_cell($Row, $Col);
            $Val = defined $Cell ? $Cell->value() : "";
            $Val =~ s/$BadC//g if $Val;
            $Val =~ s/$BadD//g if $Val;
            $emptyrow = 0 if $emptyrow && $Val ne "";
            $Val = &validate_date($Val) if ($Col == 14 || $Col == 15 || $Col == 19 || $Col == 20);
            &Helper::Log($LogF, sprintf("\tInvalid date, SupS = %s, Row = %d, Col = %d", $SupS, ($Row + 1), $Col)) unless (defined $Val);
            $Val = undef if(defined $Val && $Val eq "");
            push(@data, $Val);
        }

        next if $emptyrow;

        if (@data > 26) {
            $csv->combine(splice(@data, 26));
            my $last_values = $csv->string();
            push(@data, $last_values);
        }
        eval {
            $insert_stmt->execute(@data);
        };
        if ($@) {
            &Helper::Log($LogF, "\t$DBI::errstr, SupS = $SupS, Row $Row, Record is not added");
            $fail++;
        } else {
            $succ++;
        }
    }
    Helper::Log($LogF, "Success: $succ, Fail: $fail");
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