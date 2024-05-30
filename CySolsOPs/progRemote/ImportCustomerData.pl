#!/usr/bin/perl
use strict;
use warnings;
use utf8;
# use autodie;
use DBI;
use Text::CSV;
use Spreadsheet::ParseExcel;
use Encode qw(encode);
use FindBin;
use File::Spec;
use File::Path qw(make_path);
use lib "$FindBin::Bin";
require 'ComLib.pl';
require 'Helper.pl';

my ($config, $dbh, $csv, $LogF);
my  $LogFN   = 'ImportCustomerData.log';
my  $ConfF   = File::Spec->catfile($FindBin::Bin, '..', 'conf', 'CySolsOPs.conf');
    $config  = Helper::read_config($ConfF);

eval {
    init($config);
    drop_customer_table();
    create_customer_table();
    populate_customer_table($config->{CUSTMF}, get_insert_statement());
    terminate();
};
if ($@) {
    Helper::Log($LogF, "An error occurred: $@");
}

sub init {
    my $config = shift;
    make_path($config->{LOGDIR}) unless (-e $config->{LOGDIR});
    $LogF = File::Spec->catfile($config->{LOGDIR}, $LogFN);
    $dbh  = DBI->connect("dbi:SQLite:dbname=$config->{DB}", '', '', { RaiseError => 1, AutoCommit => 0, sqlite_unicode => 1 });
    $csv  = Text::CSV->new({ binary => 1, auto_diag => 1 });
}

sub terminate {
    $dbh->commit;
    $dbh->disconnect;
}

sub drop_customer_table {
    $dbh->do(q{
        DROP TABLE IF EXISTS customer
    });
    $dbh->commit;
}

sub create_customer_table {
    $dbh->do(q{
        CREATE TABLE IF NOT EXISTS customer (
            Sequence INTEGER NOT NULL,
            CustID TEXT NOT NULL,
            BPartner TEXT,
            SAgency TEXT,
            EUser TEXT,
            Dept TEXT,
            Designation TEXT,
            SAgent TEXT,
            SAMail TEXT,
            SATel TEXT,
            SAFax TEXT,
            EUPost TEXT,
            EUAddr TEXT,
            Comments TEXT,
            Changes TEXT,
            EUBusCat TEXT,
            Rest TEXT,
            PRIMARY KEY("CustID")
        )
    });
}

sub get_insert_statement {
    my $insert_stmt = $dbh->prepare(q{
        INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    });
    return $insert_stmt;
}

sub populate_customer_table {
    my ($CUSTMF, $insert_stmt) = @_;
    unless (-f $CUSTMF) {
        Helper::Log($LogF, "File not found: $CUSTMF");
        return;
    }

    my $parser = Spreadsheet::ParseExcel->new();
    my $workbook = $parser->parse($CUSTMF);
    unless (defined $workbook) {
        Helper::Log($LogF, "Parsing error: " . $parser->error() . " in file $CUSTMF");
        return;
    }

    my $worksheet = $workbook->worksheet(0);
    unless (defined $worksheet) {
        Helper::Log($LogF, "Sheet 1 not found in file $CUSTMF");
        return;
    }

    my ($col_min, $col_max) = $worksheet->col_range();
    my ($row_min, $row_max) = $worksheet->row_range();

    my $bad_c = "\x{ff0d}";
    my $bad_d = "\x{fffd}"; # Unmappable characters

    my ($succ, $fail) = (0, 0);
    for my $row (1 .. $row_max) {
        my @data = ();
        my $empty_row = 1;

        for my $col (0 .. $col_max) {
            my $cell = $worksheet->get_cell($row, $col);
            my $val = defined $cell ? $cell->value() : "";
            $val =~ s/$bad_c//g if $val;
            $val =~ s/$bad_d//g if $val;
            $empty_row = 0 if $empty_row && $val ne "";
            $val = undef if ($val eq "");
            push(@data, $val);
        }

        next if $empty_row;

        if (@data > 17) {
            $csv->combine(splice(@data, 17));
            my $last_values = $csv->string();
            push(@data, $last_values);
        }
        eval {
            $insert_stmt->execute(@data);
        };
        if ($@) {
            Helper::Log($LogF, "Row $row, " . $DBI::errstr . ", Record is not added");
            $fail++;
        } else {
            $succ++;
        }
    }
    Helper::Log($LogF, "Success: $succ, Fail: $fail");
}