#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use DBD::SQLite;
use Text::CSV;
use Time::HiRes qw(gettimeofday tv_interval);
use POSIX qw(strftime); 

my ($csv, $db, $dbh, $ATL, $Terms, $EncodingI, $EncodingO, $repeat);
my  $LogF          = "./ATLTDbProg.log";

&Init();
&DropTable();
&CreateTable();

my $s_time = [gettimeofday()];
my $startTimeStr = strftime("%Y/%m/%d %H:%M:%S", localtime($s_time->[0]));
&Log(sprintf("Started ATLT at %s\n", $startTimeStr));

&InsertRecords();
my ($data_ref, $expRes_ref) = ReadTerms();
&CheckConflict($data_ref, $expRes_ref);

sub Init {
    ($EncodingI, $EncodingO) = ("SJIS", "utf8");
    $csv = Text::CSV->new({ binary => 1, auto_diag => 1 });
    $db  = "./ATLTest.db";
    $ATL = "./ATL.csv";
    $Terms = "./Terms.csv";
    $repeat = 1000;
    $dbh = DBI->connect("dbi:SQLite:dbname=$db", "", "", { RaiseError => 1, AutoCommit => 0 });
}

sub DropTable {
    $dbh->do("DROP TABLE IF EXISTS ATL");
    print "Table dropped if it existed\n";
}

sub CreateTable {
    $dbh->do("CREATE TABLE IF NOT EXISTS ATL (
                mac_address TEXT,
                ip_address TEXT,
                terminal_name TEXT,
                description TEXT,
                period TEXT,
                vlan TEXT,
                UNIQUE(mac_address, ip_address, vlan)
              )");
#   $dbh->do("CREATE INDEX IF NOT EXISTS idx_mac_address_ip_address_vlan ON ATL (mac_address, ip_address, vlan)");
    print "Table Created with indices\n";
}

sub InsertRecords {
    my $insert_stmt = $dbh->prepare(q{
        INSERT INTO ATL VALUES (?, ?, ?, ?, ?, ?)
    });
    my $count = 0;

    open(my $fh, "< :encoding($EncodingO)", "$ATL") or die "Cannot open file $ATL: $!";
    while (my $row = $csv->getline($fh)) {
        $insert_stmt->execute(@$row);
        $count++;
    }
    close($fh);
    $dbh->commit;
    print "Inserted records\n";
    &Log("Read $count recs, $ATL\n");
}

sub ReadTerms{
    my (@data, @expRes,);
    my $count = 0;
    open(my $fh, "< :encoding($EncodingO)", "$Terms") or die "Cannot open file $Terms: $!";
    while (my $row = $csv->getline($fh)) {
        my ($mac, $ip, $vlan, $expRes) = @$row;
        push @data, [$mac, $ip, $vlan];
        push @expRes, $expRes;
        $count++;
    }
    close($fh);
    &Log("Read $count Terms, $Terms\n");
    return (\@data, \@expRes);
}

sub CheckConflict {
    my ($data_ref, $expRes_ref) = @_;
    my $start_time = [gettimeofday()];

    # Single query to handle all combinations
    my $sth = $dbh->prepare("
        SELECT COUNT(*) FROM ATL WHERE
        (mac_address = ? AND ip_address = ? AND vlan = ?) OR
        (mac_address = '' AND ip_address = ? AND vlan = ?) OR
        (mac_address = ? AND ip_address = '' AND vlan = ?) OR
        (mac_address = '' AND ip_address = '' AND vlan = ?) OR
        (mac_address = ? AND ip_address = ? AND vlan = '-ALL-') OR
        (mac_address = '' AND ip_address = ? AND vlan = '-ALL-') OR
        (mac_address = ? AND ip_address = '' AND vlan = '-ALL-') OR
        (mac_address = '' AND ip_address = '' AND vlan = '-ALL-')
    ");

    for (my $r = 0; $r < $repeat; $r++) {
        my ($succ, $fail) = (0, 0);
        foreach my $i (0 .. $#$data_ref) {
            my ($mac, $ip, $vlan) = @{$data_ref->[$i]};
            my $count = 0;
            
            # Execute the query with placeholders for all combinations
            $sth->execute($mac, $ip, $vlan, $ip, $vlan, $mac, $vlan, $vlan,
                          $mac, $ip, $ip, $mac);
            $count = $sth->fetchrow_array();
            
            if ($count == $expRes_ref->[$i]) {
                $succ++;
            } else {
                $fail++;
            }
        }
        print "Round: $r Successes: $succ, Failures: $fail\n";
        &Log(sprintf("Round %4d Succ %d Fail %d\n", $r, $succ, $fail)) if ($r == 0);
    }

    $sth->finish();
    $dbh->disconnect();

    my $milliseconds = tv_interval($start_time) * 1000;
    my $endTimeStr = strftime("%Y/%m/%d %H:%M:%S", localtime());
    &Log(sprintf("Finished ATLT at %s\n", $endTimeStr));
    &Log(sprintf("Time taken for %4d rounds: $milliseconds ms\n", $repeat));
}

sub Log
{
    my ($Msg) = @_;
    die "Could not open $LogF $!"
         unless open (LOGF, ">>$LogF");
    print LOGF $Msg;
    close LOGF;
}
