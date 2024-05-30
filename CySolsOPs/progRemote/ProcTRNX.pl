#!/usr/bin/perl
use strict;

$ENV{TERM}  = 'dumb' if ! exists $ENV{TERM};
my  $DateStr= shift;
my  $Option = shift || "";
use FindBin;
use File::Spec;
use DBI;
use Text::CSV;
use Encode qw(encode decode);
use lib "$FindBin::Bin";
require 'CySolsOpsLib.pl';
require 'Helper.pl';

my ($config, $dbh, $csv, $LogF);
my (@Txns);
my  $LogFN   = 'ProcTRNX.log';
my  $ConfF   = File::Spec->catfile($FindBin::Bin, '..', 'conf', 'CySolsOPs.conf');
    $config  = Helper::read_config($ConfF);

eval {
    init($config);
    get_vouchers($config);
    my $TxnsSZ = scalar(@Txns);
    Helper::Log($LogF, "Processing $TxnsSZ vouchers");
    update_customer_data($config, \@Txns);
    terminate();
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

sub terminate {
    $dbh->disconnect();
}

sub get_vouchers {
    my $config = shift;
    die "Could not open $config->{TRNDIR} $!" unless opendir (DIR, $config->{TRNDIR});
    @Txns = grep { /ProdRegn-\d+-\d+\S+\.xls$/ } readdir DIR;
    closedir DIR;
}

sub update_customer_data
{
    my ($config, $Txns) = @_;
    Helper::Log($LogF, "Updating customer data");
    my ($added, $updated, $exists) = (0, 0, 0);
    for my $Txn (sort @$Txns)
    {
        my  $OrdF    = "$config->{TRNDIR}/$Txn";
        my ($Res, $Msg) = &CySolsOpsLib::GetProdRegnDetsXls($OrdF);

        my $sth = $dbh->prepare("SELECT * FROM customer WHERE CustID = ?");
        $sth->execute($CySolsOpsLib::OCstID);
        my $row = $sth->fetchrow_hashref();

        my $CustMod = 0;
        if ($row) {
            $CustMod = 1 if (
                (defined $CySolsOpsLib::ORPartner && defined $row->{BPartner} && ($CySolsOpsLib::ORPartner ne $row->{BPartner})) ||
                (defined $CySolsOpsLib::OCPartner && defined $row->{SAgency} && ($CySolsOpsLib::OCPartner ne $row->{SAgency})) ||
                (defined $CySolsOpsLib::OCstName  && defined $row->{EUser} && ($CySolsOpsLib::OCstName  ne $row->{EUser})) ||
                (defined $CySolsOpsLib::OCstDept  && defined $row->{Dept} && ($CySolsOpsLib::OCstDept  ne $row->{Dept})) ||
                (defined $CySolsOpsLib::OCstDesn  && defined $row->{Designation} && ($CySolsOpsLib::OCstDesn  ne $row->{Designation})) ||
                (defined $CySolsOpsLib::OConName  && defined $row->{SAgent} && ($CySolsOpsLib::OConName  ne $row->{SAgent})) ||
                (defined $CySolsOpsLib::OConEMail && defined $row->{SAMail} && ($CySolsOpsLib::OConEMail ne $row->{SAMail})) ||
                (defined $CySolsOpsLib::OTel      && defined $row->{SATel} && ($CySolsOpsLib::OTel      ne $row->{SATel})) ||
                (defined $CySolsOpsLib::OFax      && defined $row->{SAFax} && ($CySolsOpsLib::OFax      ne $row->{SAFax})) ||
                (defined $CySolsOpsLib::OEUPost   && defined $row->{EUPost} && ($CySolsOpsLib::OEUPost   ne $row->{EUPost})) ||
                (defined $CySolsOpsLib::OEUAddr   && defined $row->{EUAddr} && ($CySolsOpsLib::OEUAddr   ne $row->{EUAddr})) ||
                (defined $CySolsOpsLib::OComments && defined $row->{Comments} && ($CySolsOpsLib::OComments  ne $row->{Comments})) ||
                (defined $CySolsOpsLib::OChanges  && defined $row->{Changes} && ($CySolsOpsLib::OChanges  ne $row->{Changes})) ||
                (defined $CySolsOpsLib::OBusCat   && defined $row->{EUBusCat} && ($CySolsOpsLib::OBusCat   ne $row->{EUBusCat}))
            );
            unless ($CustMod) {
                Helper::Log($LogF, "\tDid not add $CySolsOpsLib::OCstID:$CySolsOpsLib::OCstName. Already Exists!");
                $exists++;
                next;
            }

            my $update_sth = $dbh->prepare("UPDATE customer SET
                BPartner = ?, SAgency = ?, EUser = ?, Dept = ?, Designation = ?, SAgent = ?, SAMail = ?,
                SATel = ?, SAFax = ?, EUPost = ?, EUAddr = ?, Comments = ?, Changes = ?, EUBusCat = ?
                WHERE CustID = ?");

            $update_sth->execute($CySolsOpsLib::ORPartner, $CySolsOpsLib::OCPartner, $CySolsOpsLib::OCstName,
                                 $CySolsOpsLib::OCstDept,  $CySolsOpsLib::OCstDesn,  $CySolsOpsLib::OConName,
                                 $CySolsOpsLib::OConEMail, $CySolsOpsLib::OTel,      $CySolsOpsLib::OFax,
                                 $CySolsOpsLib::OEUPost,   $CySolsOpsLib::OEUAddr,   $CySolsOpsLib::OComments,
                                 $CySolsOpsLib::OChanges,  $CySolsOpsLib::OBusCat,   $CySolsOpsLib::OCstID);

            Helper::Log($LogF, "\tUpdated $CySolsOpsLib::OCstID info in CustomerMaster");
            $updated++;
        } else {
            my $insert_sth = $dbh->prepare("INSERT INTO customer (Sequence, CustID, BPartner, SAgency, EUser, Dept, Designation,
                SAgent, SAMail, SATel, SAFax, EUPost, EUAddr, Comments, Changes, EUBusCat) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            my $last_seq_sth = $dbh->prepare("SELECT MAX(Sequence) FROM customer WHERE Sequence IS NOT NULL");
            $last_seq_sth->execute();
            my ($CustLastSeq) = $last_seq_sth->fetchrow_array();
            $CustLastSeq = 0 unless defined $CustLastSeq;

            $insert_sth->execute($CustLastSeq + 1, $CySolsOpsLib::OCstID,    $CySolsOpsLib::ORPartner,
                                 $CySolsOpsLib::OCPartner,   $CySolsOpsLib::OCstName,  $CySolsOpsLib::OCstDept,
                                 $CySolsOpsLib::OCstDesn,    $CySolsOpsLib::OConName,  $CySolsOpsLib::OConEMail,
                                 $CySolsOpsLib::OTel,        $CySolsOpsLib::OFax,      $CySolsOpsLib::OEUPost,
                                 $CySolsOpsLib::OEUAddr,     $CySolsOpsLib::OComments, $CySolsOpsLib::OChanges,
                                 $CySolsOpsLib::OBusCat);

            Helper::Log($LogF, "\tAdded Cust $CustLastSeq + 1, $CySolsOpsLib::OCstID $CySolsOpsLib::OCstName to CustomerMaster");
            $added++;
        }
    }
    Helper::Log($LogF, "Added $added, Updated $updated, Already exists: $exists");

}

# sub UpdtInvAddSupp
# {
#     my  ($Res, $Msg, $SSeq) = (0, "", undef);
#     my  ($PCKey, $OProdCodeO, $OProdCode, $ProdCodeCnt) = (undef, undef, undef, 0);
#     my  ($OSerial); 
#     for  $PCKey (sort keys %CySolsOpsLib::OProdCodes)
#     {
#          $OProdCodeO= $CySolsOpsLib::OProdCodes{$PCKey};
#          $ProdCodeCnt++;
#          $OProdCode = $OProdCodeO->{prodCode};
#          $OSerial   = $OProdCodeO->{serials};
#          if     ($OProdCode =~ /WB0AX/ ||
#                  $OProdCode =~ /BB0AX/ ||
#                  $OProdCode =~ /VB0AX/ ||
#                  $OProdCode =~ /VB4AX/   )
#          {
#                 ($Res, $Msg, $SSeq) = &ARMAXUpdtInvAddSupp($PCKey, $OProdCode,$OSerial);
#                  &Log($LogFD, $Msg. "\n");
#          }
#          elsif (&MatchNK4_NK5_NSK_OProdCode($OProdCode))
                 
#          {
#                 ($Res, $Msg, $SSeq) = &MGA1UUpdtInvAddSupp($PCKey, $OProdCode,$OSerial);
#                  &Log($LogFD, $Msg. "\n");
#          }
#          else
#          {
#              &Log($LogFR, "Inventory and Support will be manually updated for $OProdCode, $OSerial\n");
#              &Log($LogFD, "Inventory and Support will be manually updated for $OProdCode, $OSerial\n");
#          }
#          last if ($Res);
#     }
#     return (0, "Updated Inventory and added Supp recs for $ProdCodeCnt ProdCodes");
# }

# sub ARMAXUpdtInvAddSupp
# {
#     my ($PCKey, $OProdCode,$OSerial) = @_;
#     my  @Serials = split ',',$OSerial;
#     my ($Res, $Msg, $Msg1, $Msg2, $Serial, $OldSerial, $Ind);
#     my  $OProdCodeO  = $CySolsOpsLib::OProdCodes{$PCKey};
#     my  $OldSerialsS = $OProdCodeO->{oldSerials};
#     my  @OldSerials  = split ',', $OldSerialsS;
#     # for $Ind (0..$#Serials)
#     # {
#     #       $Serial     = $Serials   [$Ind];
#     #       $OldSerial  = $OldSerials[$Ind];
#     #      ($Res, $Msg) = &CySolsOpsLib::UpdtNanoInvRec($PCKey, $OProdCode, $Serial, $OldSerial);
#     #       if ($Res)
#     #       {
#     #           $Msg1 = "Error occured. Txn = $Txn; Serial = $Serial. $Msg ";
#     #           print "Error occured. Txn = $Txn; Serial = $Serial. $Msg ";
#     #           return ($Res, $Msg1);
#     #       }
#     #       $Msg1 .= "\n" .$Msg
#     # }
#     return ($Res, $Msg1) unless   (($CySolsOpsLib::OSupConNo =~ /\S+/ &&
#                                     $CySolsOpsLib::OSupSDate =~ /\S+/ && 
#                                     $CySolsOpsLib::OSupEDate =~ /\S+/    ) ||
#                                    ($CySolsOpsLib::OSupConNo =~ /TBD/i &&
#                                     $CySolsOpsLib::OSupSDate =~ /TBD/i && 
#                                     $CySolsOpsLib::OSupEDate =~ /TBD/i    ));
#     $Msg1 .= "\n";   
#    ($Res, $Msg2) = &CySolsOpsLib::UpdtSuppXls($PCKey, $OProdCode, \@Serials, \@OldSerials, $UpdtLF); 
#     $Msg1 .= "\n" .$Msg2;
#     return ($Res, $Msg1);
# }

# sub MGA1UUpdtInvAddSupp
# {
#     my ($PCKey, $OProdCode,$OSerial ) = @_;
#     my  @Serials = split ',',$OSerial;
#     my ($Res, $Msg1, $Msg2, $Serial);
#     my  $OProdCodeO  = $CySolsOpsLib::OProdCodes{$PCKey};
#     my  $OldSerialsS = $OProdCodeO->{oldSerials};
#     my  @OldSerials  = split ',', $OldSerialsS;
#     # for   $Serial (@Serials)
#     # {
#     #     #($Res, $Msg1) = &CySolsOpsLib::UpdtNanoInvRec($PCKey, $PProdCode, $Serial);
#     #      $Msg1  = "Inventory will be updated manually for " unless ($Msg1 =~ /^Inventory/);
#     #      $Msg1 .= " $OProdCode $OSerial ";
#     #     #if ($Res)
#     #     #{ 
#     #     #    $Msg1 = "Error occured. Txn = $Txn; Serial = $Serial. $Msg1 ";
#     #     #     print "Error occured. Txn = $Txn; Serial = $Serial";
#     #     #     return ($Res, $Msg1);
#     # }
#     return ($Res, $Msg1) unless  (($CySolsOpsLib::OSupConNo =~ /\S+/ &&
#                                    $CySolsOpsLib::OSupSDate =~ /\S+/ && 
#                                    $CySolsOpsLib::OSupEDate =~ /\S+/    ) ||
#                                   ($CySolsOpsLib::OSupConNo =~ /TBD/i &&
#                                    $CySolsOpsLib::OSupSDate =~ /TBD/i && 
#                                    $CySolsOpsLib::OSupEDate =~ /TBD/i    )   );
#     $Msg1 .= "\n";
#    ($Res, $Msg2) = &CySolsOpsLib::UpdtSuppXls($PCKey, $OProdCode, \@Serials, \@OldSerials, $UpdtLF); 
#     $Msg1 .= "\n" . $Msg2;
#     return ($Res, $Msg1);
# }

# sub UpdtSuppXls
# {
#     my ($PCKey,$ProdCode,$Serials, $OldSerials, $UpdtLogF) = @_;
#     my ($AllowVern, $DelvVern) = ($OAllowVern, $ODelvVern);
#     my  $OProdCodeO = $OProdCodes{$PCKey};
#     $AllowVern = $OProdCodeO->{allowVern} if (exists $OProdCodeO->{allowVern} &&
#                                                      $OProdCodeO->{allowVern} =~ /\S+/);
#     $DelvVern  = $OProdCodeO->{delvVern}  if (exists $OProdCodeO->{delvVern}  &&
#                                                      $OProdCodeO->{delvVern}  =~ /\S+/);
#     $OLicCnt   = $OProdCodeO->{licCnt}    if (exists $OProdCodeO->{licCnt}    &&
#                                                      $OProdCodeO->{licCnt}    =~ /\d+/);
#     my ($SupConNo, $SupSDate, $SupEDate) = ("", "", "");
#     $SupConNo  = $OSupConNo unless ($OSupConNo =~ /TBD/i);
#     $SupSDate  = $OSupSDate unless ($OSupSDate =~ /TBD/i);
#     $SupEDate  = $OSupEDate unless ($OSupEDate =~ /TBD/i);
#     my  ($Serial,  $OldSerial )= (0, 0);
#     my  (@Serials, @OldSerials, $CPSKey, $PSKey, $CPKey, $OCPSKey, $SProdCode);
#     @Serials =  @$Serials; @OldSerials = @$OldSerials;
#     my   $Ind;
#     $Serials[0] = "ABCDEFGHIJKLMNOPQRSTUVWZYZ" if  ($#Serials < 0);
#     for  $Ind (0..$#Serials)
# }

# sub DoValidateTxns
# {
#     my  ($Errors, $Msgs, $Msg) = (0, "", "");
#     for $Txn (sort @Txns)
#     {
#         print "Validating $Txn\n";
#         &Log( $LogFD, "=======>  Validating $Txn ===================>\n");
#         my  $OrdF    = "$TRND/$Txn";
#         my ($Res, $Msg) = &CySolsOpsLib::ValidateProdRegnDetsXls($OrdF);
#         $Errors += $Res;
#         $Msgs   .= ($Txn . " " . $Msg) if ($Res);
#         print $Msg, "\n" if ($Res);
#         &Log( $LogFR, "Invalid $Txn ===================>\n") if ($Res);
#         &Log( $LogFD, "Invalid $Txn ===================>\n") if ($Res);
#     }
#     return ($Errors, $Msgs);
# }