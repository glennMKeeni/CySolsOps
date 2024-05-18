#!/usr/bin/perl
use strict;
use warnings;
use Text::CSV;
use Time::HiRes qw(gettimeofday tv_interval);
use POSIX qw(strftime); 
my  $csv= Text::CSV->new ({ binary => 1, eol => $/ });
my ($AllVLans)     = "-ALL-";
my (%WTermIPVLs,%WTermIPV4s,%WTermIPV6s,%RegTerms, $StartT,$Rec, $Recs);
# my  $LogF          = "/tmp/ATLTProg.log";
# my  $WTermsF = "/db0/NetSkateKoban/conf/WhiteList_eth0.csv";
# my  $TermsF  = "/tmp/Terms.csv";
my  $LogF          = "./ATLTProg.log";
my  $WTermsF = "./ATL.csv";
my  $TermsF  = "./Terms.csv";
my (@Terms, @Res, $Ind);

my  $Rounds  = 1000;
my  $CRound  = 1;

my $start_time = [gettimeofday()];
my $startTimeStr = strftime("%Y/%m/%d %H:%M:%S", localtime($start_time->[0]));
&Log(sprintf("Started ATLT at %s\n", $startTimeStr));
&ReadWhiteTerms($WTermsF);
&ReadTerms($TermsF);

while ($CRound <= $Rounds) {
    printf("Round %3d\n", $CRound);
    &DoCheckWL();
    $CRound++;
}

my $ETime = tv_interval($start_time) * 1000;
my $endTimeStr = strftime("%Y/%m/%d %H:%M:%S", localtime());
printf("Time taken for %3d rounds: %s ms\n", $Rounds, $ETime);
&Log(sprintf("Finished ATLT at %s\n", $endTimeStr));
&Log(sprintf("Time taken for %3d rounds: %s ms\n", $Rounds, $ETime));

sub ReadWhiteTerms
{
    my ($WTerms, $Dups, $Recs, $Rec, $WTERMFH, $StartT, $ElT, $WMacIPVLan, $WMacVLan, $IPV4);
    undef %WTermIPVLs;
    undef %WTermIPV4s;
    undef %WTermIPV6s;
    undef %RegTerms;
    $StartT = time;
    $Dups = 0;
    die  "Could not open $WTermsF $!"
          unless open ($WTERMFH, $WTermsF);
    while ($Rec = $csv->getline($WTERMFH))
    {
         $Recs++;
         $RegTerms{"ATLRecs"} = $Recs;
         next if (($#{$Rec} < 0)|| ($$Rec[0] =~ /^\s*#/ ));
       # 2011/10/14 14:06:28,ARP,00:0d:29:09:9e:42,1,192.168.0.7,0,0,Summit
       # my  ($WMAC, $WIP, $WName, $WDescr, $WB, $TimeSpecs) = split ',', $Rec;
         my  ($WMAC, $WIP, $WName, $WDescr, $TimeSpecs, $WVLan) = @$Rec;
         $WName =~ s/^\s*//;
         $WName =~ s/\s*$//;
         $WVLan       = $AllVLans if ($WVLan =~ /^\s*$/);
         # if there is a VLAN in Whitelist that is 
         #    not in MonVLan - it must be ignored!
         $WMacIPVLan  = "$WMAC-$WIP-$WVLan";
         $WMacVLan    = "$WMAC-$WVLan";
         $RegTerms{$WMacVLan} = $RegTerms{"ATLRecs"} if ($WMAC !~ /^\s*$/);
         if  (! exists $WTermIPVLs{$WMacIPVLan})
         {
              $csv->combine($WDescr,$WName,$TimeSpecs);
              $WTermIPVLs{$WMacIPVLan} = $csv->string();
            # &CheckAndPushToWTermIPV4s($WMAC, $WIP) if ($WIP =~ /\d+\.\d+\.\d+\.\d+/);
              $WTermIPV6s{$WMAC} = $WIP              if ($WIP =~ /^[a-fA-F0-9:]+$/);      # Check! Glenn
              $WTerms++;
         }
         else
         {
              &Log ("Ignoring Rec $Rec\n");
              $Dups++;
         }
    }
    close $WTERMFH;
    $ElT = time - $StartT;
    &Log ("Read $Recs recs, $WTerms White terms [$Dups Duplicates] ($ElT secs) $WTermsF\n");
    return ($WTerms);
}
sub GetMatchedWTerm
{
    my ($DTermIPVL, $Time, $WDay, $HrMin) = @_;
    my  $DTermVLan; 
  # my  $DTermVLan       = $DTermIPs{$DTermIP}->{"detVLanID"};
  #     $DTermVLan       = "-ALL-" if  ($DTermVLan =~ /^\s*$/);

    my ($mac, $ip, $vl ) = split /-/, $DTermIPVL;
        $DTermVLan       = $vl;                      # Check Logic Changes! Glenn
        $DTermVLan       = "-ALL-" if  ($DTermVLan =~ /^\s*$/);
    my @candidates = (
        join('-', $mac, $ip, $DTermVLan),
        join('-', $mac, '',  $DTermVLan),
        join('-', '',   $ip, $DTermVLan),
        join('-', '',   '',  $DTermVLan)
    );
    push @candidates,
       (join('-', $mac, $ip, '-ALL-'),
        join('-', $mac, '',  '-ALL-'),
        join('-', '',   $ip, '-ALL-'),
        join('-', '',   '',  '-ALL-')
    ) unless ($DTermVLan eq $AllVLans);

    my  $lastStatus = 1;
    my ($DTermIPVLan, $WTermIPV4);
    for $DTermIPVLan (@candidates) {
        if (exists $WTermIPVLs{$DTermIPVLan}) {
            my $WTermIPVLDets = $WTermIPVLs{$DTermIPVLan};
            $lastStatus       = 0; 
            if ($lastStatus  == 0) {
                return (0, $DTermIPVLan);
            }
        }
    }

    {
        my  $WTermIPV4s    = $WTermIPV4s{$mac};
        for $WTermIPV4 (@$WTermIPV4s)
        {
            my @candidates = (
                join('-', $mac, $WTermIPV4, $DTermVLan),
                join('-', $mac, '',         $DTermVLan)
            );
            push @candidates,
               (join('-', $mac, $WTermIPV4, '-ALL-'),
                join('-', $mac, '',         '-ALL-')
            ) unless ($DTermVLan eq $AllVLans);
            for $DTermIPVLan (@candidates)
            {
                next unless (exists $WTermIPVLs{$DTermIPVLan});
                my $WTermIPDets  = $WTermIPVLs{$DTermIPVLan};
                $lastStatus      = &CheckTimeSpec($WTermIPDets, $Time, $WDay, $HrMin);
                if ($lastStatus == 0) {
                    return (0, $WTermIPV4);
                }
            }
        }
    }
    return ($lastStatus, undef);
}

sub ReadTerms
{
    my ($TermsF) = @_;
    my  $Seq = 0;
    die "Could not open $TermsF $!"
         unless open (TERMSF, $TermsF);
    while (<TERMSF>)
    {
         chomp;
         $Seq++;
         my ($MAC, $IP, $VLan, $ERes) = split ',';
         push @Terms, "$MAC-$IP-$VLan";
         push @Res,    $ERes;
    }
    close TERMSF;
    &Log("Read $Seq Terms\n");
}

sub DoCheckWL
{
    my  $Seq = 0;
    my ($Pass, $Fail) = (0, 0);
    for $Seq (0..$#Terms)
    {
        #my ($MAC, $IP, $VLan) = split("-", $Terms[$Seq]);
         my ($SRes,$Term) = &GetMatchedWTerm($Terms[$Seq]); 
         my  $ERes        = $Res[$Seq];
        #printf ("%3d  $Term $MAC, $IP, $VLan\n", $Seq) 
         $Pass++         if ($ERes == 1 && $SRes == 0);
        #printf ("%3d  FAIL $MAC, $IP, $VLan\n", $Seq) 
         $Fail++         if ($ERes ==  $SRes );
    }
    &Log("Pass: $Pass, Fail: $Fail\n") if ($CRound == 1);
}

sub Log
{
    my ($Msg) = @_;
    die "Could not open $LogF $!"
         unless open (LOGF, ">>$LogF");
    print LOGF $Msg;
    close LOGF;
}
