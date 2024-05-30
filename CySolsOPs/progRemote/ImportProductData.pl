#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Text::CSV;
# use utf8;
use FindBin;
use File::Spec;
use File::Path qw(make_path);
use lib "$FindBin::Bin";
require 'ComLib.pl';
require 'Helper.pl';

my ($config, $dbh, $csv, $LogF);
my  $LogFN   = 'ImportProductData.log';
my  $ConfF   = File::Spec->catfile($FindBin::Bin, '..', 'conf', 'CySolsOPs.conf');
    $config  = Helper::read_config($ConfF);

eval {
    &init($config);
    &drop_product_table();
    &create_product_table();
    &populate_product_table($config->{PRODMF}, get_insert_statement());
    &terminate();
    1;
} or do {
    my $error = $@ || 'Unknown error';
    Helper::Log($LogF, "Error: $error");
};

sub init {
    my ($config) = shift;
    make_path($config->{LOGDIR}) unless -e $config->{LOGDIR};
    $LogF = File::Spec->catfile($config->{LOGDIR}, $LogFN);
    $dbh = DBI->connect("dbi:SQLite:dbname=$config->{DB}", '', '', { RaiseError => 1, AutoCommit => 1, sqlite_unicode => 1 });
    $csv = Text::CSV->new({ binary => 1, auto_diag => 1 });
}

sub terminate {
    # $dbh->commit;
    $dbh->disconnect;
}

sub drop_product_table {
    $dbh->do(q{
        DROP TABLE IF EXISTS product
    });
}

sub create_product_table {
    $dbh->do(q{
        CREATE TABLE IF NOT EXISTS product (
            ProdCode TEXT PRIMARY KEY NOT NULL,
            ProdName TEXT,
            Model TEXT
        )
    });
}

sub get_insert_statement {
    my $p_insert_stmt = $dbh->prepare(q{
        INSERT INTO product VALUES (?, ?, ?)
    });
    return $p_insert_stmt;
}

sub populate_product_table {
    my ($PRODMF, $insert_stmt) = @_;

    unless (-f $PRODMF) {
        Helper::Log($LogF, "File not found: $PRODMF");
        return;
    }

    open(my $fh, "< :encoding(SJIS)", $PRODMF) or die "Cannot open file $PRODMF: $!";
    my ($ln, $succ, $fail) = (0, 0, 0);
    while (my $row = $csv->getline($fh)) {
        $ln = $ln + 1;
        next if($ln == 1);
        my $num_columns = scalar(@$row);
        eval{
           $insert_stmt->execute(@$row);
        };
        if ($@) {
            Helper::Log($LogF, "Row $ln, " . $DBI::errstr . ", Record is not added");
            $fail++;
        } else {
            $succ++;
        }
    }
    close($fh);
    Helper::Log($LogF, "Success: $succ, Fail: $fail");
}