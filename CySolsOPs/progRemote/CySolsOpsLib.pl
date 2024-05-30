#!/usr/bin/perl
$ENV{TERM} = 'dumb' if ! exists $ENV{TERM};
package CySolsOpsLib;
use strict;
use Time::Local 'timelocal_nocheck';
use Text::CSV;
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::SaveParser;
# use Text::Iconv;
use File::Copy;
use FindBin;
use lib "$FindBin::Bin";
use Encode qw(encode);
require 'ComLib.pl';
my  @Files;
#my $BDIR   = "/";
#my $BDIR   = "//CSS-WAREHOUSE/Support/CySolsOpsToSoumu/CySolsOpsInv/";
 my $BDIR   = "C:/Users/ISS-S/Documents/TASKS/CySolOPs/CySolsOpsToSoumu/CySolsOpsInv/";
#my $BDIR   = "//192.168.0.193/Support/CySolsOpsToSoumu/CySolsOpsInv/"; # This does not work!
my  $DDIR   = $BDIR . "CySolsOps/";
my  $TDIR   = $DDIR . "tmp/";
my  $CSVD   = $DDIR;
my  $ARCD   = $DDIR . "Arch/";
# my  $CMFile = $CSVD . "�ڋq�}�X�^.xls";
# my  $CMFileO= $ARCD . "�ڋq�}�X�^.xls";
my  $CMFile = $CSVD . "CUSTCopy.xls";
my  $CMFileO= $ARCD . "CUSTCopy.xls";
my  $ProdToSupF    = $DDIR . "PRODCODEvsSUPPORTFILESandSHEETS.csv";
my  $NanoSuppFile  = $CSVD . "��ϼ��_Support.xls";
my  $NanoSuppFileO = $ARCD . "��ϼ��_Support.xls";
my  $MGA1USuppFile = $CSVD . "Manager�A�v���C�A���X_Support.xls";
my  $MGA1USuppFileO= $ARCD . "Manager�A�v���C�A���X_Support.xls";
my  $SupStatusF    = $DDIR . "SupportStatus.xls";
my  $NSFile       = $CSVD . "NameStrings.csv";
my  $Inv_NanoF    = $CSVD . "NK4-WB0AX_�݌�.xls";
my  $Inv_NanoFO   = $ARCD . "NK4-WB0AX_�݌�.xls";
my  $InvStatsF    = $CSVD . "InventoryStats.csv";
my  $InvInitF     = $CSVD . "Inventory-20150701.csv";
my  $OFile        = $TDIR . "ProdStats.csv";       # Product Statistics
my  $SFile        = $TDIR . "SupStatus.csv";       # Support Status
my  $SFileBP      = $TDIR . "SupStatusByP.csv";    # Support Status by Product
my  $SFileBPH     = $TDIR . "SupStatusByP.html";   # Support Status by Productin html
my  $SFileBPNew   = $TDIR . "SupStatusByPNew.csv"; # Support Status by Product
my  $SFileBPHNew  = $TDIR . "SupStatusByPNew.html";# Support Status by Productin html
my  $OFileH = $TDIR . "ProdStats.html";      # Product Statistics in html
my  $SFileH = $TDIR . "SupStatus.html";      # Support Status in html
my  $PFile  = $TDIR . "1ProductList.csv";    # Product List
my  $PCatF  = $TDIR . "1ProdCatList.csv";    # Product Category List
my  $CustF  = $TDIR . "1CustomerList.csv";   # Customer List
my  $Option = shift || "ALL";
my  $ResF   = $TDIR . "Res-$Option.csv";
my  $LogF   = $TDIR . "Log-$Option.csv";
my  $ErrF   = $TDIR . "Errors.csv";
my (@MFiles, $MFile);
our (%Customers, %ProdStats, %ProdCStats, %SupCons, $SupPnYMPr, %SupPnYMPrs, %DeletedIDs, %SupSeqs);
our (%CustByName, %NanoWBInvRecs, %NanoWBInvSerials,%NanoBBInvRecs, %NanoBBInvSerials, %OProdCodes, 
     %ProdCodes, %SupDB);
our ($OCstID,    $ORPartner, $OCPartner, $OCstName, $OCstDept, $OCstDesn,   $OConName,
     $OConEMail, $OTel,      $OFax,      $OEUPost,  $OEUAddr,  $OChanges,   $OBusCat, 
     $OProdCode, $OSerial,   $OLicCnt,   $ODelvDate,$ORegnDate,$OAllowVern, $ODelvVern,
     $OSupConNo, $OSupSDate, $OSupEDate, $OComments);
our ($SSeq,      $SBlank,    $SCstID,    $SRPartner,$SCPartner,$SCstName,   $SCstDept,
     $SCstDesn,  $SConName,  $SProdCode, $SSerial,  $SAcct,    $SPass,      $SLicCnt,
     $SDelvDate, $SUseSDate, $SAllowVern,$SDelvVern,$SSupConNo,$SSupSDate,  $SSupEDate,
     $SComments);# 22 fields
our ($CPRegF, $CPRegDateS);   # The current Product Registration file
my   $SupFlds    = 24;
my  ($InvCumIn,$InvSaiSei,$InvHenPin,$InvCumDelv,$InvHenKyaku,$InvDemoEtcInternal,$InvMonBal);
my  ($CustLastSeq, %SSerials, %PSerials, %CustProd);
my  (%BPs);
our (%OFlds, %Inv, @SupFs, %ProdToSupF);
my ($CYear, $CMon, $CDay);
my ($secs,$mins,$hrs,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
$CYear = $year + 1900; $CMon = $mon + 1; $CDay = $mday;
my  $UpDtTimeS = sprintf "%4d-%02d-%02d %02d:%02d:%02d", $CYear,$CMon,$CDay,$hrs,$mins,$secs;
my ($UpDtFTimeS,  $UpDtFTag);
my  $CsvF  = Text::CSV->new( { binary => 1, eol => $/, quote_binary => 0, quote_space => 0 } );
my ($EncodingI, $EncodingO) = ("SJIS", "utf8");
my ($CataLogCol,$SerialNoCol, $AccountCol,  $PasswdCol, $LicCntCol) = (9,10,11,12, 13); 
my ($DelvDateCol, $SupConNoCol, $SupSDateCol,  $SupEDateCol, $SupCommCol) = (14, 18, 19, 20, 21);
binmode(STDOUT, ":utf8");
mkdir $TDIR unless (-d $TDIR);
mkdir $ARCD unless (-d $ARCD);
&GetCusts();
sub InitDBLoad
{
    my ($Res, $Msg, $ProdCodesP);
    ($secs,$mins,$hrs,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
     $CYear = $year + 1900; $CMon = $mon + 1; $CDay = $mday;
     $UpDtTimeS = sprintf "%4d-%02d-%02d %02d:%02d:%02d", $CYear,$CMon,$CDay,$hrs,$mins,$secs;
     $UpDtFTimeS= sprintf "%4d%02d%02d_%02d%02d%02d",     $CYear,$CMon,$CDay,$hrs,$mins,$secs;
     mkdir $TDIR unless (-d $TDIR);
     my $LAInd  = &GetLastArchIndex($UpDtFTimeS);
     $LAInd++;
     $UpDtFTag  = sprintf ("%s_%02d", $UpDtFTimeS, $LAInd);
    ($OCstID,    $ORPartner, $OCPartner, $OCstName, $OCstDept, $OCstDesn,   $OConName,
     $OConEMail, $OTel,      $OFax,      $OEUPost,  $OEUAddr,  $OChanges,   $OBusCat, 
     $OProdCode, $OSerial,   $OLicCnt,   $ODelvDate,$ORegnDate,$OAllowVern, $ODelvVern,
     $OSupConNo, $OSupSDate, $OSupEDate, $OComments) = 
     (undef,     undef,      undef,      undef,     undef,     undef,       undef,
      undef,     undef,      undef,      undef,     undef,     undef,       undef,
      undef,     undef,      undef,      undef,     undef,     undef,       undef,
      undef,     undef,      undef,      undef);
    ($SSeq,      $SBlank,    $SCstID,    $SRPartner,$SCPartner,$SCstName,   $SCstDept,
     $SCstDesn,  $SConName,  $SProdCode, $SSerial,  $SAcct,    $SPass,      $SLicCnt,
     $SDelvDate, $SUseSDate, $SAllowVern,$SDelvVern,$SSupConNo,$SSupSDate,  $SSupEDate,
     $SComments)= 
     (undef,     undef,      undef,      undef,     undef,     undef,       undef,
      undef,     undef,      undef,      undef,     undef,     undef,       undef,
      undef,     undef,      undef,      undef,     undef,     undef,       undef,
      undef);
    ($CustLastSeq, %SSerials, %PSerials, %OFlds, %CustProd) = (undef, (), (), (), ());
    (%Customers, %ProdStats, %ProdCStats, %SupCons, $SupPnYMPr, %SupPnYMPrs, %DeletedIDs) =
    ((),         (),         (),          (),       undef,      (),          ());
    (%CustByName, %NanoWBInvRecs, %NanoWBInvSerials, %NanoBBInvRecs, %NanoBBInvSerials, %OProdCodes) = ((), (), (), ()); 
    (%ProdToSupF) = ();
     @SupFs       = ();
    ($Res, $Msg) = &LoadProdToSupDets();
     return ($Res, $Msg) if ($Res); 
    ($Res, $Msg) = &GetNanoInvRecs("WB");
     return ($Res, $Msg) if ($Res); 
    ($Res, $Msg) = &GetNanoInvRecs("BB");
     return ($Res, $Msg) if ($Res); 
     return (0, "Inited DB load UpDtFTag = $UpDtFTag");
}

sub GetMFiles
{
    die "Could not opendir $CSVD $!"
         unless opendir (DIR, $CSVD);
   (@MFiles) = grep { (/.csv/) } readdir (DIR);
    closedir(DIR);
}

sub PrintMFiles
{
    for $MFile (sort @MFiles)
    {
        print $MFile, "\n";
    }
}

sub GetCusts
{
     my  ($CMFILE, $CustID, $Rec, $Recs,$CRec, $row, $col, $CustBP, $CustNo);
     my  ($CRec,$CMFILE, $Msg, $EUserN);
     my  ($KabuS, $BrktS) = &LoadNameStrings();
     my   @KabuS          = @$KabuS;
     my   @BrktS          = @$BrktS;
     my   $Parser = Spreadsheet::ParseExcel->new();
     my   $eBook  = $Parser->parse($CMFile);
     $Msg = "GetCusts ";
     (%Customers, %CustByName, $CustLastSeq) = (undef, undef, undef);
     if (! defined $eBook)
     {
          return (1, "Could not process $CMFile; $!");
     }
     my $sheets = $eBook->{SheetCount};
     my ($sheet,  $eSheet, $sheetName, $row, $column) = (0, undef, undef);
     $eSheet    = $eBook->{Worksheet}[$sheet];
     $sheetName = $eSheet->{Name};
     if  ((! exists ($eSheet->{MaxRow})) || (! exists ($eSheet->{MaxCol})))
     {
          return (1, "$Msg No Data in $CMFile");
     }
     foreach $row ($eSheet->{MinRow} .. $eSheet->{MaxRow}) {
         my @Flds;
         foreach $column ($eSheet->{MinCol} .. $eSheet->{MaxCol}) {
             if (defined $eSheet->{Cells}[$row][$column])
             {
                 push @Flds, $eSheet->{Cells}[$row][$column]->Value; 
             } else {
                 push @Flds, "";
             }
         }
         $Recs ++;
         my ($Seq,  $CustID,$BPartner,$SAgency, $EUser,   $Dept,    $Designation,$SAgent,$SAMail,
             $SATel,$SAFax, $EUPost,  $EUAddr,  $Comments,$Changes, $EUBusCat,   $Rest) = @Flds;
        my $text = "($Seq,  $CustID,$BPartner,$SAgency, $EUser,   $Dept,    $Designation,$SAgent,$SAMail,
             $SATel,$SAFax, $EUPost,  $EUAddr,  $Comments,$Changes, $EUBusCat,   $Rest)\n";
        my $encoded_text = encode("UTF-8", $text);
        print $encoded_text;
         my  %CDBRec;
        #printf "%3d,%s,%30s,%30s,%30s,%10s\n",$Seq,$CustID,$BPartner,$SAgency,$EUser;
         $BPartner =~ s/\"//g;
         $SAgency  =~ s/\"//g;
         $SAgent   =~ s/\"//g;
         $EUser    =~ s/\"//g;
         $EUserN   = $EUser;
         my ($Kabu, $Brkt);
         for $Kabu (@KabuS)
         {
             $EUserN =~ s/$Kabu//;
         }
         for $Brkt (@BrktS)
         {
             if  ($EUserN =~ /$Brkt(.*)\S$/)
             {
                  $EUserN = $1;
             }
         }
         %CDBRec   = (bPartner=> $BPartner, sAgency   => $SAgency,     sAgent => $SAgent,
                      eUser   => $EUser,    eUBusCat  => $EUBusCat,    seq    => $Seq,
                      eUDept  => $Dept,     eUDesn    => $Designation, eUMail => $SAMail,
                      eUTel   => $SATel,    eUFax     => $SAFax,       eUPost => $EUPost,  
                      eUAddr  => $EUAddr,   comments  => $Comments,    changes=> $Changes, 
                      eUBusCat=> $EUBusCat, row       => $row
                      );
         if  (exists $Customers{$CustID})
         {
              $Msg .= "Duplicates: " unless ($Msg =~ /Duplicate/); 
              $Msg .= "[$CustID, $Seq, " . $Customers{$CustID}->{seq} . "];"; 
         }
         $Customers{$CustID}  =  \%CDBRec;
         $CustByName{$EUserN} =  $CustID;
         $CustLastSeq         =  $Seq  if ((!defined $CustLastSeq) || ($CustLastSeq < $Seq));
        ($CustBP, $CustNo)    =  $CustID   =~ /(CK\d\d\d)(\d+)/;
         $BPs{$CustBP}        =  $CustID   unless (exists $BPs{$CustBP} &&
                                                         ($BPs{$CustBP} gt $CustID));
     }
     $Msg .= "\n" if ($Msg =~ /Duplicates/);
     $Msg .= "Loaded $Recs Customers ";
     return (0, $Msg);
}

sub PrintCusts
{
    my ($CustID, $CUSTF, $Seq);
    die "Could not open $CustF $!"
         unless open ($CUSTF, "> :encoding($EncodingO)", $CustF);
    for $CustID (sort keys %Customers)
    {
        $Seq++;
        my  $CustRO = $Customers{$CustID};
        printf "%3d,%s,%30s,%30s,%30s,%10s\n",$Seq,$CustID,
                $CustRO->{bPartner},$CustRO->{sAgency},$CustRO->{eUser};
        printf $CUSTF "%d,%s,%s,%s,%s,%s\n",$Seq,$CustID,
                $CustRO->{bPartner},$CustRO->{sAgency},$CustRO->{eUser};
    }
    close $CUSTF;
}

sub PrintProds
{
    die "Could not open $PFile $!"
       # unless open (PFILE, "> :encoding($EncodingO)", $PFile);
         unless open (PFILE, ">                      ", $PFile);
    for $MFile (@MFiles)
    {
        my $PProd = $MFile;
        $PProd  =~ s/�̔�-�ێ�_���_1\.csv//;
        $PProd  =~ s/�̔�-�ێ�_���\.csv//;
        $PProd  =~ s/-�ێ�_��ꗗ\.csv//;
        $PProd  =~ s/�̔�-�ێ�_��� _1\.csv//;
        $PProd  =~ s/-�ێ�_�񖢉���\.csv//;
        $PProd  =~ s/^\d+\-\d+\-//;
        print PFILE $PProd, "\n";
    }
    close PFILE;
}

sub PrintProdCats
{
    die "Could not open $PCatF $!"
       # unless open (PCATF, "> :encoding($EncodingO)", $PCatF);
         unless open (PCATF, ">                      ", $PCatF);
    my  $PCat;
    for $PCat (sort keys %ProdCStats)
    {
        my ($PProd, $Cat)  = split ' ', $PCat;
        print       $PCat, " ", $ProdCStats{$PCat}->{"totP"}, " ", $ProdCStats{$PCat}->{"totL"}, "\n";
        print PCATF $PCat, " ", $ProdCStats{$PCat}->{"totP"}, " ", $ProdCStats{$PCat}->{"totL"}, "\n";
    }
    close PCATF;
}

sub GetProdStats 
{
    my ($IDIndex, $PNIndex,$CLIndex,$CVIndex,$CNIndex, $CSIndex, $CEIndex) = (2,4,13,16,18,19,20);
    my ($TIndex,  $LIndex, $StatsF, $DetsF) = (2,3,0,0);
    for $MFile (@MFiles)
    {
        next unless ($MFile =~ /^\d+\-\d+\-/);
        next if     ($MFile =~ /^0\-/);   # Cust and Partner Masters
        my ($Index, $PRec);
        my  $PProd = $MFile;
        $PProd  =~ s/�̔�-�ێ�_���_1.csv//;
        $PProd  =~ s/-�ێ�_��ꗗ.csv//;
        $PProd  =~ s/-�̔�-�ێ�_��� _1.csv//;
        $ProdStats{$MFile}->{"prod"} = $PProd;
        $ProdStats{$MFile}->{"totP"} = -1;
        $ProdStats{$MFile}->{"totL"} = -1;
        $ProdStats{$MFile}->{"curL"} = -1;
        $ProdStats{$MFile}->{"finL"} = -1;
        $ProdStats{$MFile}->{"addL"} = -1;

        my $MFileN = $CSVD . $MFile;
        my $RecNo;
        if  ($MFile =~ /^5\-1\-/)
        {
              print "Got KenMon\n";
        }
        die "Could not open $MFileN $!"
             unless open (MFILE, $MFileN);
        while (<MFILE>)
        {
             $RecNo++;
             chop;
             s/\"(\d+),(\d+)\"/$1$2/g;
             if  ((/^#\s*,\s*,���C�Z���X��\s*,\s*�ێ�_��/))
             {
                 $StatsF = 1;
                  next;
             }
             if  ((/^#\s*,\s*,�ڋq�h�c\s*,/))
             {
                 $DetsF = 1;
                  next;
             }
             if  (($StatsF) && /^#\s*,\s*,\s*(\d+)\s*,\s*(\d+)\s*/)
             {
                  $ProdStats{$MFile}->{"totP"} = $1;
                  $ProdStats{$MFile}->{"totL"} = $1;
                  $ProdStats{$MFile}->{"curL"} = $2;
                  if   (/^#\s*,\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d*)\s*,\s*(\d+)\s*/)
                  {
                          $ProdStats{$MFile}->{"finL"} = $3;
                          $ProdStats{$MFile}->{"addL"} = $4;
                  }
                  $StatsF = 0;
                  next;
             }
             next unless ($DetsF);
              my  @Fields = &splitCSVLine($_);
              my  @PFields= &splitCSVLine($PRec);
              $Fields[$IDIndex] = $PFields[$IDIndex] if (($Fields[$IDIndex] !~ /\S+/) && ($PFields[$IDIndex] =~ /\S+/));
              if  (($Fields[$IDIndex] !~ /\S+/) ||
                   ($Fields[$CSIndex] !~ /\S+/) ||
                   ($Fields[$CEIndex] !~ /\S+/) ||
                   ($Fields[$CNIndex] !~ /\S+/) ||
                   ($Fields[$CVIndex] !~ /\S+/)    )
              {
                    &LogErr ("$MFile,$RecNo,NoVal". $_ . "\n");
                    next;
              }
              {
                    for $Index ($IDIndex, $CSIndex, $CEIndex, $CNIndex, $CVIndex)
                    {
                        $Fields[$Index] =~ s/\"//g;
                        $Fields[$Index] =~ s/^\s*//g;
                        $Fields[$Index] =~ s/\s*$//g;
                    }
                    my $CustID = $Fields[$IDIndex];
                    my $SCon   = $Fields[$CSIndex];
                    my $ECon   = $Fields[$CEIndex];
                    my $NCon   = $Fields[$CNIndex];
                    my $PName  = $Customers{$CustID}->{"sAgency"};
                    my $CVern  = $Fields[$CVIndex];
                    my $LCount = $Fields[$CLIndex];
                    my $BCat   = $Customers{$CustID}->{"eUBusCat"};
                    my $CStat  = "EXPIRED";
                    if (($Option ne "ALL") && ($Option eq  $CustID))
                    {
                         &LogRes ( "$PProd, $CustID, $SCon, $ECon, $NCon, $PName, $CVern\n");
                    }
                    if  (exists $SupCons{$CustID . " " . $PProd})
                    {
                         my $OMFile = $SupCons{$CustID . " " . $PProd}->{"mFile"};
                         my $OECon  = $SupCons{$CustID . " " . $PProd}->{"eCon"};
                         my $ONCon  = $SupCons{$CustID . " " . $PProd}->{"nCon"};
                         next if ( $OECon >= $ECon);
                         &LogErr ("DUP, $PProd, $CustID, $NCon, $ONCon, $RecNo:$MFile, $OMFile \n");
                    }
                    if  ($CustID =~ /\-D$/)
                    {
                         &LogErr ("Del, $PProd, $CustID, $NCon, $RecNo:$MFile\n");
                         my $OCustID = $CustID;
                         $OCustID    =~ s/\-D$//;
                         $DeletedIDs{$OCustID} = 1;
                         next;
                    }
                    next if (exists $DeletedIDs{$CustID});
                    $PName     =~ s/\"//g;
                    my  ($EYear, $EMon, $EDay) = split '/', $Fields[$CEIndex];
                    my   $CTime = timelocal_nocheck (0,0,0,$CDay,$CMon-1,$CYear-1900);
                    my   $ETime = timelocal_nocheck (0,0,0,$EDay,$EMon-1,$EYear-1900);
                    $SupPnYMPrs{$PName . (sprintf ",%d,%02d,%s,$CustID", $EYear, $EMon, $CustID,$PProd) } = 
                                                                 $CustID . " " . $PProd; # The Key

                    
                    if  ($ETime > $CTime)
                    {
                         $CStat = "OK";
                         if  (($ETime - $CTime) < (180*24*60*60))
                         {
                               $CStat .= "-lt-6Months";
                         }
                    }
                    if  ($ETime <= $CTime)
                    {
                         $CStat = "EXPIRED";
                         if  (($CTime - $ETime) < (180*24*60*60))
                         {
                               $CStat .= "-lt-6Months";
                         }
                    }
                    $SupCons{$CustID . " " . $PProd}->{"sCon"} = $Fields[$CSIndex];
                    $SupCons{$CustID . " " . $PProd}->{"cStat"}= $CStat;
                    $SupCons{$CustID . " " . $PProd}->{"eCon"} = $Fields[$CEIndex];
                    $SupCons{$CustID . " " . $PProd}->{"nCon"} = $Fields[$CNIndex];
                    $SupCons{$CustID . " " . $PProd}->{"pName"}= $PName;
                    $SupCons{$CustID . " " . $PProd}->{"cVern"}= $CVern;
                    $SupCons{$CustID . " " . $PProd}->{"mFile"}= $RecNo . ":" . $MFile;
                    $ProdCStats{$BCat. " " . $PProd}->{"lCount"} += $LCount;
                    $PRec = $_;
              }
              
        }
        close MFILE;
        &Log ("Read $RecNo recs from $PProd \n");
    }
}

sub PrintProdStats
{
    my ($Prod);
    die "Could not open $OFile $!"
         unless open (OFILE, ">$OFile");
    die "Could not open $OFileH $!"
         unless open (OFILEH, ">$OFileH");
    print OFILEH "<html>\n",
        "<head>\n",
        "    <title>Products and Licenses delivered</title>\n",
        "    <meta http-equiv=\"content-type\" content=\"text/html; charset=SHIFT_JIS\" />\n",
        "    <!-- **** layout stylesheet **** -->\n",
        "    <link rel=\"stylesheet\" type=\"text/css\" href=\"AccessStats/style/style.css\" />\n",
        "    <!-- **** colour scheme stylesheet **** -->\n",
        "    <link rel=\"stylesheet\" type=\"text/css\" href=\"AccessStats/style/colour.css\" />\n",
        "<center> <img src=\"AccessStats/images/koban_for_catv_banner.png\"> </center>\n",
        "</head>\n",
        "<body>\n",
        "<div id=\"logo\"><h1></h1></div>\n",
        "<hr><ul>\n";
    print  OFILEH "<table border=\"1\">\n",
                  "<tr>\n",
                  "<td>Product Name</td>","<td>�o�א�</td>","<td>���C�Z���X��</td>\n",
                  "<td>�ێ�_��</td>","<td>�_��I��</td>","<td>�ێ疢����</td>\n",
                  "</tr>\n";
   
    printf OFILE "%30s,%s,%s,%s,%s,%s\n", "���i��", "�o�א�", "���C�Z���X��", "�ێ�_��","�_��I��","�ێ疢����";
    for $Prod (sort keys %ProdStats)
    {
        my  $PProd = $ProdStats{$Prod}->{"prod"};
        my  $TotP  = $ProdStats{$Prod}->{"totP"};
        my  $TotL  = $ProdStats{$Prod}->{"totL"};
        my  $CurL  = $ProdStats{$Prod}->{"curL"};
        my  $FinL  = $ProdStats{$Prod}->{"finL"};
        my  $AddL  = $ProdStats{$Prod}->{"addL"};
        printf OFILE 
              "%s,%4d,%4d,%4d,%4d,%4d\n", $PProd,  $TotP, $TotL, $CurL, $FinL, $AddL;
        print  OFILEH 
              "<tr>\n",
                  "<td>$PProd</td>","<td>$TotP</td>","<td>$TotL</td>\n",
                  "<td>$CurL</td>","<td>$FinL</td>","<td>$AddL</td>\n",
                  "</tr>\n"
                                                     
    } 
    print OFILEH "</table>\n", 
                 "</table>\n",
                 "</html>\n";
    close OFILE;
    close OFILEH;
}
sub PrintSupStats
{
    die "Could not open $SFile $!"
         unless open (OFILE, ">$SFile");
    die "Could not open $SFileH $!"
         unless open (OFILEH, ">$SFileH");
    print OFILEH "<html>\n",
        "<head>\n",
        "    <title>Status of Support Licenses</title>\n",
        "    <meta http-equiv=\"content-type\" content=\"text/html; charset=SHIFT_JIS\" />\n",
        "    <!-- **** layout stylesheet **** -->\n",
        "    <link rel=\"stylesheet\" type=\"text/css\" href=\"AccessStats/style/style.css\" />\n",
        "    <!-- **** colour scheme stylesheet **** -->\n",
        "    <link rel=\"stylesheet\" type=\"text/css\" href=\"AccessStats/style/colour.css\" />\n",
        "<center> <img src=\"AccessStats/images/koban_for_catv_banner.png\"> </center>\n",
        "</head>\n",
        "<body>\n",
        "<div id=\"logo\"><h1></h1></div>\n";
    print  OFILEH "<table border=\"1\">\n",
                  "<tr>\n",
                  "<td>Seq</td>","<td>Customer Product</td>","<td>���� Ver</td>","<td>�_��J�n��</td>","<td>������</td>\n",
                  "<td>��</td>","<td>�ێ�_�� No</td>","<td>�㗝�X ��</td>","<td>End User ��</td>\n",
                  "</tr>\n";
    printf OFILE "%s,%30s,%s,%s,%s,%s,%s,%s,%s\n", "Seq", "Customer Product","���� Ver", "�_��J�n��", "������", 
                                          "��", "�ێ�_�� No", "�㗝�X ��", "End User ��";
    my  ($SupCon, $Seq);

    $Seq = 1;
    for $SupCon (sort SortRtn keys %SupCons)
    {
         my  ($EYear, $EMon, $EDay) = split '/', $SupCons{$SupCon}->{"eCon"};
         my   $CStat = $SupCons{$SupCon}->{"cStat"};
         my   $SCon  = $SupCons{$SupCon}->{"sCon"};
         my   $ECon  = $SupCons{$SupCon}->{"eCon"};
         my   $NCon  = $SupCons{$SupCon}->{"nCon"};
         my   $PName = $SupCons{$SupCon}->{"pName"};
         my   $CVern = $SupCons{$SupCon}->{"cVern"};
         my  ($CustID, $Prod) = split ' ', $SupCon, 2;
         my   $EUser = $Customers{$CustID}->{"eUser"};
         printf OFILE 
              "%3d,%s,%s,%s,%s,%s,%s,%s,%s\n", $Seq, $SupCon, $CVern, $SCon, $ECon, $CStat,$NCon, $PName, $EUser;
         print  OFILEH "<tr>\n",
                  "<td>$Seq</td>","<td>$SupCon</td>","<td>$CVern</td>","<td>$SCon</td>","<td>$ECon</td>\n",
                  "<td>$CStat</td>","<td>$NCon</td>","<td>$PName</td>","<td>$EUser</td>\n",
                  "</tr>\n";
         $Seq++; 
         if   ($Seq == 222)
         {
               print "The Seq is 222\n";
         }                                          
    }
    print OFILEH "</table></body></html>\n";
    close OFILE;
    close OFILEH;
    die "Could not open $SFileBP $!"
         unless open (OFILE, ">$SFileBP");
    die "Could not open $SFileBPH $!"
         unless open (OFILEH, ">$SFileBPH");
    print OFILEH "<html>\n";
    print OFILEH 
        "<head>\n",
        "    <title>Status of Support Licenses by Partner </title>\n",
        "    <meta http-equiv=\"content-type\" content=\"text/html; charset=SHIFT_JIS\" />\n",
        "    <!-- **** layout stylesheet **** -->\n",
        "    <link rel=\"stylesheet\" type=\"text/css\" href=\"AccessStats/style/style.css\" />\n",
        "    <!-- **** colour scheme stylesheet **** -->\n",
        "    <link rel=\"stylesheet\" type=\"text/css\" href=\"AccessStats/style/colour.css\" />\n",
        "<center> <img src=\"AccessStats/images/koban_for_catv_banner.png\"> </center>\n",
        "</head>\n",
        "<body>\n",
        "<div id=\"logo\"><h1></h1></div>\n",
        "<h1 style=\"text-align:center\">Support Status by Business Partner</h1>\n",
        "<h1 style=\"text-align:center\">Updated $UpDtTimeS</h1>\n",;
    
    print  OFILEH "<table border=\"1\">\n",
                  "<tr>\n",
                  "<td>Seq</td>","<td>�㗝�X ��</td>","<td>CVern</td>","<td>Customer Product</td>","<td>�_��J�n��</td>","<td>������</td>\n",
                  "<td>��</td>","<td>�ێ�_�� No</td>","<td>End User ��</td>\n",
                  "</tr>\n";
   #printf OFILE "%30s,%s,%s,%s,%s,%s\n", "Seq", "�㗝�X ��", "���� Ver", "Customer Product", "�_��J�n��", "������", 
   #                                      "��", "�ێ�_�� No",  "End User ��";
    printf OFILE "%s,%s,%s,%s,%s,%s\n","�㗝�X ��","�_��J�n��", "������", 
                                         "��", "�ێ�_�� No",  "End User ��";
    my  ($PMon, $PPName,$PSeq);
    for  $SupPnYMPr (sort {$b cmp $a} keys %SupPnYMPrs)
    {
         my ($PName, $Year, $Mon, $CustIDK, $Prod) = split ',', $SupPnYMPr;
         my  $YearMon = $Year . $Mon;
       # next if (($YearMon > 201208) || ($YearMon < 201104));
       # $Seq = 1 if ((! defined $PMon) || ($PMon != $Mon) || ($PName ne $PPName));
         $Seq = 1 if (($PName ne $PPName));
         $PSeq++  if (($PName ne $PPName));
        
         $SupCon = $SupPnYMPrs{$SupPnYMPr};
         my   $CStat = $SupCons{$SupCon}->{"cStat"};
         my   $SCon  = $SupCons{$SupCon}->{"sCon"};
         my   $ECon  = $SupCons{$SupCon}->{"eCon"};
         my   $NCon  = $SupCons{$SupCon}->{"nCon"};
         my   $PName = $SupCons{$SupCon}->{"pName"};
         my   $CVern = $SupCons{$SupCon}->{"cVern"};
         my  ($CustID, $Prod) = split ' ', $SupCon, 2;
         my   $EUser = $Customers{$CustID}->{"eUser"};
         my   $PCStat = $CStat;
         my   $BPSeq  = "$PSeq.$Seq";
         $PCStat  = "<td style=\"color:red;text-align:center\">$CStat</td>";
         $PCStat  = "<td style=\"color:blue;text-align:center\">$CStat</td>"   if  ($CStat eq "OK-lt-6Months");
         $PCStat  = "<td style=\"color:red;text-align:center\">$CStat</td>"    if  ($CStat eq "EXPIRED-lt-6Months");
         $PCStat  = "<td style=\"color:green;text-align:center\">$CStat</td>"  if  ($CStat eq "OK");
         printf OFILE 
              "%3d,%s,%s,%s,%s,%s,%s,%s,%s\n", $Seq, $PName, $CVern, $SupCon,  $SCon, $ECon, $CStat,$NCon, $EUser;
         print  OFILEH "<tr>\n",
                  "<td>$BPSeq</td>","<td>$PName</td>","<td>$CVern</td>","<td>$SupCon</td>","<td>$SCon</td>","<td>$ECon</td>\n",
                   $PCStat,"<td>$NCon</td>","<td>$EUser</td>\n",
                  "</tr>\n";
         $Seq++;
         $PMon = $Mon; $PPName = $PName;                                         
    }
    print OFILEH "</table>\n", 
                 "</html>\n";
    close OFILE;
    close OFILEH;
}

sub Log
{
    my   ($Msg) = @_;
    die  "Could not open  $ResF $! "
          unless open (LOGF, ">> $LogF");
    my    $TS = &ComLib::GetLogTS(time);    
    print LOGF "$TS $Msg";
    close LOGF;
}

sub LogRes
{
    my   ($Msg) = @_;
    die  "Could not open  $ResF $! "
          unless open (LOGF, ">> $ResF");
    print LOGF $Msg;
    close LOGF;
}

sub LogErr
{
    my   ($Msg) = @_;
    die  "Could not open  $ErrF $! "
          unless open (LOGF, ">> $ErrF");
    print LOGF $Msg;
    close LOGF;
}

sub SortRtn
{
    my ($aKey, $aStat, $bKey, $bStat);
    return ($a cmp $b);      # Kamewada request
    $aKey = $a;
    $bKey = $b;
    $aStat = $SupCons{$aKey}->{"cStat"};
    $bStat = $SupCons{$bKey}->{"cStat"};
    my $A  = sprintf "%16s%s", $aStat,$aKey;
    my $B  = sprintf "%16s%s", $bStat,$bKey;
    return ($B cmp $A);
}

sub splitCSVLine {
    my ($tmp) = @_;
    $tmp =~ s/(?:\x0D\x0A|[\x0D\x0A])?$/,/;
    my @values =
      map { /^"(.*)"$/ ? scalar( $_ = $1, s/""/"/g, $_ ) : $_ }
      ( $tmp =~ /("[^"]*(?:""[^"]*)*"|[^,]*),/g );
    return @values;
}

sub LoadNameStrings
{
    my  $RecNo = 0;
    my ($NSFILE, $CRec);
    my (@KabuS, @BrktS);
    die "Could not open $NSFile $!"
         unless open ($NSFILE, "< :encoding($EncodingI)", $NSFile);
    while ( $CRec = $CsvF->getline($NSFILE) )
    {
        $RecNo++;
        my @Flds = @$CRec;
        my $Fld;
        for $Fld (@Flds)
        {
            push @KabuS, $Fld     if  ($RecNo == 1);
            push @BrktS, $Fld     if  ($RecNo == 2);
        }
    }
    close $NSFILE;
    return (\@KabuS, \@BrktS);
}

sub GetNanoInvRecs
{
    my  ($NanoT) = @_;
    my  ($INVF, $CRec, $RecNo, $Recs, $Dups) = (undef, undef, 0,0,0);
    my  ($row,  $col);
    my  ($Res,  $Msg) = (0,"GetNanoInvRecs for NanoType $NanoT\n");
    my  ($NanoInvRecs, $NanoInvSerials);
    my   $Parser= Spreadsheet::ParseExcel->new();
    my   $eBook = $Parser->parse($Inv_NanoF);
    if (! defined $eBook)
    {
         return (1, "Could not process $Inv_NanoF; $!");
    }
    my  $sheets = $eBook->{SheetCount};
    my ($sheet,   $eSheet, $sheetName, $row, $column) = (0, undef, undef,0,0);
    if    ($NanoT eq "WB")
    {
           $sheet            = 0;
           $NanoInvRecs      = \%NanoWBInvRecs;
           $NanoInvSerials   = \%NanoWBInvSerials;
    }
    elsif ($NanoT eq "BB")
    {
           $sheet            = 1;
           $NanoInvRecs      = \%NanoBBInvRecs;
           $NanoInvSerials   = \%NanoBBInvSerials;
    }
    else
    {
          die "Unknown NanoType in GetNanoInvRecs $NanoT [Expected: WB/BB]\n";
    }
    undef %$NanoInvRecs;
    undef %$NanoInvSerials;
    $eSheet     = $eBook->{Worksheet}[$sheet];
    $sheetName  = $eSheet->{Name};
    if  ((! exists ($eSheet->{MaxRow})) || (! exists ($eSheet->{MaxCol})))
    {
         return (1, "No Data in $Inv_NanoF $sheetName") unless ($sheetName eq "BB");
         return (0, "No Data in $Inv_NanoF $sheetName. Update BB Inventory!"); # 
    }
    foreach $row ($eSheet->{MinRow} .. $eSheet->{MaxRow}) {
        my (@Flds,$Serial, $Mac, $IDate, $DDate, $CustID,
            $User,$Partner, $PType, $PUsage, $Com) ;
        $Recs++;
        foreach $col ($eSheet->{MinCol} .. $eSheet->{MaxCol}) {
            if (defined $eSheet->{Cells}[$row][$col])
            {
                push @Flds, $eSheet->{Cells}[$row][$col]->Value; 
            } else {
                push @Flds, "";
            }
        }
        ($RecNo, $Serial, $Mac, $IDate, $DDate, $CustID,$User,
                        $Partner, $PType, $PUsage, $Com) = @Flds;
        next unless ($Serial =~ /\d+/);
        $Serial  = sprintf "%06d",$Serial if ($Serial =~ /^\d+$/);
        my %InvR = (recNo => $RecNo, serial => $Serial, mac    => $Mac, 
                    iDate => $IDate, dDate  => $DDate,  custID => $CustID,
                    user  => $User,  partner=> $Partner,pType  => $PType, 
                    pUsage=> $PUsage,com    => $Com);
        my $NRecNo = $RecNo;
        $NRecNo    = 0 if ($RecNo !~ /^\d+$/ );
        if  ($RecNo == 968)
        {
        #    print ("This is the record\n");
        }
 
        if  (exists $$NanoInvSerials{$Serial})
        {
             $Msg .= "Duplicate records in Inventory " if ($Msg =~ /^GetNanoInvRecs for NanoType \S+\s*$/);
             $Msg .= "Dup InvRec for serial = $Serial RecNos: $RecNo; " . $$NanoInvSerials{$Serial} . "\n";
             $Dups ++;
        }
        else
        {
             $$NanoInvRecs   {$NRecNo} = \%InvR;        
             $$NanoInvSerials{$Serial} = $NRecNo;
        }
    }    
    return ($Dups, "Read  $RecNo ($Recs) recs [$Dups Dups] from Inventory\n$Msg ");
}

sub PutNanoInvRecs
{
    my ($NanoT);   
    my  $Inv_NanoFA   =  $Inv_NanoFO;
    $Inv_NanoFA       =~ s/\.xls$/\.$UpDtFTag\.xls/;
    SysCopy ($Inv_NanoF, $Inv_NanoFA) if (-f $Inv_NanoF);
    my ($NanoInvRecs, $SheetN );
    my $workbook      = Spreadsheet::WriteExcel->new($Inv_NanoF);
    undef  %Inv;
    $SheetN           = "WB";
    $NanoInvRecs      = \%NanoWBInvRecs;
    &UpdateNanoTSheet($workbook,$SheetN,$NanoInvRecs);
    $SheetN           = "BB";
    $NanoInvRecs      = \%NanoBBInvRecs;
    &UpdateNanoTSheet($workbook,$SheetN,$NanoInvRecs); 
}

sub UpdateNanoTSheet{
    my ($workbook,$SheetN,$NanoInvRecs) = @_;
    my  $Recs       = 0;
    my ($INVF, $RecNo, $Serial );
    my $worksheet   = $workbook->add_worksheet($SheetN);    # GetWorkSheetName!

    for  $RecNo (sort {$a <=> $b} keys %$NanoInvRecs)
    {
        $Recs ++;
        my $InvR = $$NanoInvRecs{$RecNo};
        if   ($InvR->{serial} =~ /101101\-005256/)
        {
              print ("This is the record\n");
        }
        $InvR->{iDate} =~ s/-/\//g;
        $InvR->{dDate} =~ s/-/\//g;
        my @Flds = ( $InvR->{recNo}, $InvR->{serial}, $InvR->{mac},
                     $InvR->{iDate}, $InvR->{dDate},
                     $InvR->{custID},$InvR->{user},   $InvR->{partner},
                     $InvR->{pType}, $InvR->{pUsage}, $InvR->{com});
        my  $Ind;
        my  $Dates = sprintf ("%05d %16s %10s %10s \n", $InvR->{recNo}, 
                               $InvR->{serial},
                               $InvR->{iDate}, $InvR->{dDate} );
        my  ($IY, $IM, $ID) = split '/', $InvR->{iDate};
        my  ($DY, $DM, $DD) = split '/', $InvR->{dDate};
        my   $IYM = sprintf "%d-%02d", $IY, $IM;
        my   $DYM = sprintf "%d-%02d", $DY, $DM;
        $Inv{"$IYM"}->{in}  ++ if  ($IY =~ /^\d+$/);
        $Inv{"$DYM"}->{out} ++ if  ($DY =~ /^\d+$/);                       
       #&SLog($Dates);
        for $Ind (0..$#Flds)
        {
            if  ($Ind == 1 && $Flds[$Ind] =~ /^\d+$/)
            {
                 $worksheet->write($Recs, $Ind, (sprintf "%06d", $Flds[$Ind]));
            }
            else
            {
                 $worksheet->write($Recs, $Ind, $Flds[$Ind]);
            }
        }
    }
    return (0, "Wrote $Recs recs in   Inventory");
}

sub PrintInv
{
    &ReadInitInv();
    my   $StartYM  = "2015-07";
    my   $InvRec= "YYYY-MM, In , Out, Bal\n";
    my  ($YM, $In, $Out,$TIn, $TOut) = ("", 0, 0, 0, 0);
    $TIn = $InvCumIn; $TOut = $InvCumDelv;
    $InvRec .= sprintf "%7s,%4d,%4d,%4d\n", "PrevMon", $TIn, $TOut,($TIn - $TOut);
    for  $YM (sort keys %Inv)
    {
         next unless ($YM ge $StartYM);
        ($In, $Out) = (0,0);
         $In   = $Inv{$YM}->{in}  if (exists $Inv{$YM}->{in});
         $Out  = $Inv{$YM}->{out} if (exists $Inv{$YM}->{out});
         $TIn += $In;
         $TOut+= $Out;
         $InvRec .= sprintf "%7s,%4d,%4d,%4d\n", $YM, $In, $Out,($TIn - $TOut); 
    }
    $InvRec .= sprintf "%7s,%4d,%4d,%4d\n", "Total", $TIn, $TOut,($TIn - $TOut);
    die "Could not open $InvStatsF $!"
         unless open (INVSF, ">$InvStatsF");
    print INVSF $InvRec;
    close INVSF; 
    return (0, "Generated $InvStatsF");
}

sub ReadInitInv
{
    die "Could not open $InvInitF $!"
         unless open (INVF, $InvInitF);
    while (<INVF>)
    {
         next if (/^\s*#/);
         chomp;
        ($InvCumIn,$InvSaiSei,$InvHenPin,$InvCumDelv,$InvHenKyaku,$InvDemoEtcInternal,
                                                              $InvMonBal) = split ',';
         last if (defined $InvCumIn);
    }
    close INVF;
}

sub UpdtNanoInvRec
{
    my ($PCKey, $ProdCode, $ISerial, $OldSerial)   = @_;
    my  $Updt       = 0;
    my  $Msg        = "";
    my  $Comments   = $OComments;
    my ($NanoInvRecs, $NanoInvSerials);
    $Comments       =~ s/OLD:(\S+)\s*//; 
    if    ($ProdCode =~ /WB/)
    {
           $NanoInvRecs      = \%NanoWBInvRecs;
           $NanoInvSerials   = \%NanoWBInvSerials;
    }
    elsif ($ProdCode =~ /BB/ || $ProdCode =~ /VB/  )
    {
           $NanoInvRecs      = \%NanoBBInvRecs;
           $NanoInvSerials   = \%NanoBBInvSerials;
    }
    else
    {
          die "Unknown NanoType in UpdtNanoInvRec $ProdCode [Expected: WB/BB/VB]\n";
    }

    if (defined $OldSerial && 
        exists  $$NanoInvSerials{$OldSerial} &&
                $$NanoInvRecs{$$NanoInvSerials{$OldSerial}}->{serial} eq $OldSerial)
    {
         my $InvR = $$NanoInvRecs{$$NanoInvSerials{$OldSerial}};
         $Msg  = "Got OldSerial $OldSerial in " . $InvR->{recNo} . "\n";
         $InvR->{com}   .= "-> $ISerial " . $Comments;
         $Updt           = 1;         
    }
    else
    {
         $Msg  = "Did not find OldSerial $OldSerial \n";
    }
    if (exists  $$NanoInvSerials{$ISerial} &&
                $$NanoInvRecs{$$NanoInvSerials{$ISerial}}->{serial} eq $ISerial)
    {
         my $InvR = $$NanoInvRecs{$$NanoInvSerials{$ISerial}};
         $Comments    = " $OldSerial ->" .$Comments if (defined $OldSerial);
         $Msg  = "Got Serial $ISerial in " . $InvR->{recNo} . "\n";
         $InvR->{custID} = $OCstID;
         $InvR->{user}   = $OCstName;
         $InvR->{dDate}  = $ODelvDate    unless ($ODelvDate =~ /^\s*$/ || $InvR->{dDate} =~ /\S+/ );
         $InvR->{partner}= $OCPartner;
         $InvR->{com}   .= $Comments     unless ($Comments  =~ /^\s$/);
         $InvR->{pType}  = $ProdCode;
         $Updt           = 1;
         
    }

    if ($Updt)
    {
        $Msg   = "Updt Inv for Serial $ISerial ";
        return (0, $Msg);
    }
    $Msg   = "Did not find Serial $ISerial \n";
    return (1, $Msg);
}

sub UpdtNanoInvRecs
{
    my ($Res, $Msg) = &PutNanoInvRecs();
    return ($Res, $Msg) if ($Res);
}

sub GetProdRegnDetsXls
{
    my ($PRegF) = @_;
    my ($PREGF, $Recs, $CRec, $row, $col);
    my  $Parser = Spreadsheet::ParseExcel->new();
    my   $eBook = $Parser->parse($PRegF);
    $CPRegF     = $PRegF;
   ($CPRegDateS)= $1 if ($PRegF =~ /-(\d+)-\d+\./);
   ($CPRegDateS)= $1 if ($PRegF =~ /-(\d+)-\d+_/);
    $CPRegDateS = "YYYYMMDD" unless (defined $CPRegDateS);
    %OProdCodes = ();
    if (! defined $eBook)
    {
         return (1, "Could not process $PRegF; $!");
    }
    my $sheets = $eBook->{SheetCount};
    my ($sheet,  $eSheet, $sheetName, $row, $column) = (0, undef, undef);
    $eSheet    = $eBook->{Worksheet}[$sheet];
    $sheetName = $eSheet->{Name};
    if  ((! exists ($eSheet->{MaxRow})) || (! exists ($eSheet->{MaxCol})))
    {
         return (1, "No Data in $PRegF");
    }
    foreach $row ($eSheet->{MinRow} .. $eSheet->{MaxRow}) {
        my @Flds;
        foreach $column ($eSheet->{MinCol} .. $eSheet->{MaxCol}) {
            if (defined $eSheet->{Cells}[$row][$column])
            {
                push @Flds, $eSheet->{Cells}[$row][$column]->Value; 
            } else {
                push @Flds, "";
            }
            last if ($column > 3);
        }
        my ($FldNo, $FldN, $FldV, $FldO) = @Flds;
       #$CsvF->print($TFILE, \@Flds);
        $OFlds{$FldNo} = $FldV if ($FldNo =~ /\S\d+/);
    }
    $OFlds{O16} =~ s/"//g;    # Remove quotes from the Serial number list string
   ($OCstID,    $ORPartner, $OCPartner, $OCstName, $OCstDept, $OCstDesn,   $OConName,
    $OConEMail, $OTel,      $OFax,      $OEUPost,  $OEUAddr, $OChanges,   $OBusCat, 
    $OProdCode, $OSerial,   $OLicCnt,   $ODelvDate,$ORegnDate,$OAllowVern, $ODelvVern,
    $OSupConNo, $OSupSDate, $OSupEDate, $OComments) = 
   ($OFlds{O1}, $OFlds{O2}, $OFlds{O3}, $OFlds{O4}, $OFlds{O5}, $OFlds{O6}, $OFlds{O7},
    $OFlds{O8}, $OFlds{O9}, $OFlds{O10},$OFlds{O11},$OFlds{O12},$OFlds{O13},$OFlds{O14},
    $OFlds{O15},$OFlds{O16},$OFlds{O17},$OFlds{O18},$OFlds{O19},$OFlds{O20},$OFlds{O21},
    $OFlds{O22},$OFlds{O23},$OFlds{O24},$OFlds{O25});
    if   ($OProdCode =~ /\*\*See (Det\d+)/)
    {
          my  ($Res, $Msg, $PProdCodes);
          ($Res, $Msg, $PProdCodes) = &GetProdCodes($eBook, $1);
          return (1, $Msg) if  ($Res);
          %OProdCodes = %$PProdCodes;
          return (0, "read $Msg $OSupConNo,$OComments\n");
    }
    else
    {

          $OSerial       = &GetSerial($eBook, $1)          if     ($OSerial =~ /\*\*See (Det\d+)/);
          my  $OldSerials= undef;
         ($OldSerials)   = $OComments =~ /\s*OLD:(\S+)\s*/ unless ($OSerial =~ /\*\*See (Det\d+)/);
          my   $CProdCode= $OProdCode;
          $CProdCode     =~ s/NK5/NK4/;                    # Canonicalize to old code!
          $CProdCode     =~ s/NSK/NK4/;                    # Canonicalize to old code!
          my   %ProdCode = (prodCode  => $OProdCode, serials => $OSerial, oldSerials => $OldSerials, cProdCode => $CProdCode);
          my   $PCKey    = sprintf "%02d %s", 1, $OProdCode;
          $OProdCodes{$PCKey} = \%ProdCode;
          return (0, "read $OCstID, $OProdCode, $OSerial,$OSupConNo,$OComments\n");
    } 
}


sub ValidateProdRegnDetsXls
{
    my ($PRegF) = @_;
    my ($Errs, $EMsg, $PCKey) = (0, "", undef);
    my ($Res,  $Msg)  = &GetProdRegnDetsXls($PRegF);
    if ($Res)
    {
        return (1, $Msg);
    }
    for  $PCKey (keys %OProdCodes)
    {
          my ($Res, $EMsg) = &ValidateProdAndSerial($OProdCodes{$PCKey}->{cProdCode}, 
                                                    $OProdCodes{$PCKey}->{serials},
                                                    $OProdCodes{$PCKey}->{oldSerials});
          $Msg  .= $EMsg . "\n" if ($Res);
          $Errs += $Res;
         ($Res, $EMsg) = &ValidateDate($ODelvDate, "DelvDate");
          $Msg  .= $EMsg; $Errs+= $Res;
         ($Res, $EMsg) = &ValidateDate($ORegnDate, "RegnDate");
          $Msg  .= $EMsg; $Errs+= $Res;
         ($Res, $EMsg) = &ValidateDate($OSupSDate, "SupSDate");
          $Msg  .= $EMsg; $Errs+= $Res;
         ($Res, $EMsg) = &ValidateDate($OSupEDate, "SupEDate");
          $Msg  .= $EMsg; $Errs+= $Res;
          if  ($OSupConNo =~ /\S+/)
          {
               $Msg .= "SupStartDate must be specified\n " unless ($OSupSDate =~ /\S+/);
               $Errs++                                     unless ($OSupSDate =~ /\S+/);
               $Msg .= "SupEndDate   must be specified\n " unless ($OSupEDate =~ /\S+/);
               $Errs++                                     unless ($OSupEDate =~ /\S+/);
          }
    }
    return ($Errs, $Msg);
}

sub ValidateDate
{
    my ($Date, $DateType) = @_;
    return (0, "")                          if (($Date =~ /^TBD$/i) &&
                                                ($DateType eq "SupSDate" ||
                                                 $DateType eq "SupEDate" ||
                                                 $DateType eq "RegnDate"   ));
    return (0, "")                          if (($Date =~ /^\s*$/)       &&
                                                ($DateType ne "SupSDate" &&
                                                 $DateType ne "SupEDate" &&
                                                 $DateType ne "RegnDate"   ));
    return (1, "Illegal $DateType $Date\n") if  ($Date !~ /^\d+\/\d+\/\d+/ ); 
                                           #&&   $Date !~ /^\d+\-\d+\-\d+/ );
    return (0, "");
}

sub ValidateProdAndSerial
{
    my ($ProdCode, $SerialsP, $OldSerialsP) = @_;
    my  @Serials    = split ',', $SerialsP;
    my  @OldSerials = split ',', $OldSerialsP;
    $ProdCode  =~ s/-\d*MUPG$// if ($ProdCode =~ /-\d*MUPG$/);
    return (1, "NonExistant ProdCode $ProdCode") if (!exists $ProdToSupF{$ProdCode});
    my ($Ind, $Serial, $OldSerial, $Msg, $Res) = (0, 0, 0,"", 0);
    for $Ind (0..$#Serials)
    {
        $Serial     = $Serials   [$Ind];
        $OldSerial  = $OldSerials[$Ind];
        my $NSerial = $Serial;
        $NSerial    =~ s/^S//;
        my $NOldSerial = $OldSerial;
        $NOldSerial =~ s/^S//;
        
        next if ((($ProdCode !~ /WB0AX/                  || 
                   exists $NanoWBInvSerials{$NSerial}    ||
                   exists $NanoWBInvSerials{ $Serial})   &&
                  ($ProdCode !~ /WB0AX/                  ||
                   !defined $OldSerial                   || 
                   exists $NanoWBInvSerials{$NOldSerial} ||
                   exists $NanoWBInvSerials{ $OldSerial})) &&
                 (($ProdCode !~ /[BV]B0AX/               ||
                   exists $NanoBBInvSerials{$NSerial}    ||
                   exists $NanoBBInvSerials{ $Serial})   &&
                  ($ProdCode !~ /BB0AX/                  ||
                   !defined $OldSerial                   ||
                   exists $NanoBBInvSerials{$NOldSerial} ||
                   exists $NanoBBInvSerials{ $OldSerial})) );
        $Msg .=  "NonExistant Serial $Serial ";
        $Msg .=  "Old $OldSerial " if (defined $OldSerial);
        $Msg .=  "\n";
        $Res ++;
    }
    return ($Res, $Msg) if ($Msg);
    return (0, "");
}

sub GetProdCodes
{
    my ($eBook, $DetKey) = @_;
    my ($Res, $Msg, %ProdCodes, $Prods, $Serials) = (0, "", undef, 0, "");
    undef %ProdCodes;
    my $sheets = $eBook->{SheetCount};
    my ($sheet,  $eSheet, $mySheet, $row) = (0, undef, undef,undef);
    for $sheet (1..$sheets)
    {
        next unless  ($eBook->{Worksheet}[$sheet]->{Name} eq "Items");
        $mySheet = $sheet;
        last;
    }
    $eSheet    = $eBook->{Worksheet}[$mySheet];
    if  ((! exists ($eSheet->{MaxRow})) || (! exists ($eSheet->{MaxCol})))
    {
         return (1, "No Data in Details Sheet\n", undef);
    }
    my  $PrevProdCode;
    foreach $row ($eSheet->{MinRow} .. $eSheet->{MaxRow}) {
        my  $RowP  = $eSheet->{Cells}[$row];
        my ($Seq, $ProdCode, $Serial, $LicCnt, $AllowVern, $DelvVern, $Comments) = ($RowP->[0]{Val}, 
            $RowP->[1]{Val}, $RowP->[2]{Val},$RowP->[3]{Val}, $RowP->[4]{Val}, $RowP->[5]{Val},$RowP->[6]{Val});
        next unless ($Seq =~ /^\d+/ && ($Serial =~ /\S+/ || $LicCnt =~ /\d+/));
        $ProdCode     = $PrevProdCode if (defined $PrevProdCode && $ProdCode !~ /^\S*$/);
        $PrevProdCode = $ProdCode     if ($ProdCode =~ /^\S+$/); 
        ($Res, $Msg, $Serial)         = &GetSerial($eBook, $1) if  ($Serial =~ /\*\*See (Det\d+)/);
        return (1, "Failed to get Serials. $Msg\n", undef) if ($Res);
        my  $OldSerials= undef;
       ($OldSerials)   = $Comments =~ /OLD:(\S+)\s*/ unless ($OSerial =~ /\*\*See (Det\d+)/);
        my   $CProdCode= $ProdCode;
        $CProdCode     =~ s/NK5/NK4/;   #Canonicalize to old prodCode!
        $CProdCode     =~ s/NSK/NK4/;   #Canonicalize to old code!
        my   %ProdCode = (prodCode => $ProdCode, serials   => $Serial,  oldSerials => $OldSerials,
                          licCnt   => $LicCnt,   allowVern => $AllowVern, delvVern => $DelvVern, 
                                                                          cProdCode=> $CProdCode);
        my   $PCKey    = sprintf "%02d %s", $Seq, $ProdCode;
        $ProdCodes{$PCKey} = \%ProdCode;
        $Prods++;
    }
    return (0, "Read $Prods", \%ProdCodes);
}

sub GetSerial
{
    my ($eBook, $DetKey) = @_;
    my  $sheets = $eBook->{SheetCount};
    my ($sheet,  $eSheet, $mySheet, $row) = (0, undef, undef,undef);
    for $sheet (1..$sheets)
    {
        next unless  ($eBook->{Worksheet}[$sheet]->{Name} eq "Serials");
        $mySheet = $sheet;
        last;
    }
    return (1, "Did not find 'See Det' sheet", undef) unless (defined $mySheet);
    $eSheet    = $eBook->{Worksheet}[$mySheet];
    my  $sheetName = $eSheet->{Name};
    my ($row, $Serials, $Serial, $CKey, $SCnt);
    if  ((! exists ($eSheet->{MaxRow})) || (! exists ($eSheet->{MaxCol})))
    {
         return (1, "No Data in $sheetName", undef);
    }
    foreach $row ($eSheet->{MinRow} .. $eSheet->{MaxRow}) {
        my @Flds;
        if (defined $eSheet->{Cells}[$row][0])
        {
           $CKey     = $eSheet->{Cells}[$row][0]->{Val};
           next unless ($CKey eq $DetKey);
           $Serial   = $eSheet->{Cells}[$row][1]->{Val};
           $Serials .= $Serial . ",";
           $SCnt++;
        }
    }
    $Serials =~ s/,$//;
    return (0, "Got $SCnt serials", $Serials);   
}

sub PrintProdRegnDets
{
    my  $Key;
    for $Key (1..25)
    {
        my  $OKey = sprintf ("O%d", $Key);
        printf "Fld %4s = %s\n", $OKey, $OFlds{$OKey};
    }  
}

sub AddCustRec
{
    my  $CustMod = 0;
    if  (exists $Customers{$OCstID})
    {
         my  $Seq   = $Customers{$OCstID}->{seq};
         my  $CName = $Customers{$OCstID}->{eUser};
         $CustMod = 1
         if (($ORPartner !~ /^\s*$/ && $ORPartner ne $Customers{$OCstID}->{bPartner}) ||
             ($OCPartner !~ /^\s*$/ && $OCPartner ne $Customers{$OCstID}->{sAgency})  ||
             ($OCstName  !~ /^\s*$/ && $OCstName  ne $Customers{$OCstID}->{eUser})    ||
             ($OCstDept  !~ /^\s*$/ && $OCstDept  ne $Customers{$OCstID}->{eUDept})   ||
             ($OCstDesn  !~ /^\s*$/ && $OCstDesn  ne $Customers{$OCstID}->{eUDesn})   ||
             ($OConName  !~ /^\s*$/ && $OConName  ne $Customers{$OCstID}->{sAgent})   ||
             ($OConEMail !~ /^\s*$/ && $OConEMail ne $Customers{$OCstID}->{eUMail})   ||
             ($OTel      !~ /^\s*$/ && $OTel      ne $Customers{$OCstID}->{eUTel})    ||
             ($OFax      !~ /^\s*$/ && $OFax      ne $Customers{$OCstID}->{eUFax})    ||
             ($OEUPost   !~ /^\s*$/ && $OEUPost   ne $Customers{$OCstID}->{eUPost})   ||
             ($OEUAddr   !~ /^\s*$/ && $OEUAddr   ne $Customers{$OCstID}->{eUAddr})   ||
             ($OComments !~ /^\s*$/ && $OComments ne $Customers{$OCstID}->{comments}) ||
             ($OChanges  !~ /^\s*$/ && $OChanges  ne $Customers{$OCstID}->{changes})  ||
             ($OBusCat   !~ /^\s*$/ && $OBusCat   ne $Customers{$OCstID}->{eUBusCat})   );
         return (0, "Did not add $OCstID:$OCstName. $Seq:$CName. Already Exists! \n")
                                                                    unless ($CustMod);
    }
    my $CMFileA  = $CMFileO;
    $CMFileA     =~ s/\.xls/\.$UpDtFTag\.xls/;
    SysCopy ($CMFile, $CMFileA) if (-f $CMFile);
    $CustLastSeq ++;
    my  @CFlds = 
       ($CustLastSeq,$OCstID,    $ORPartner, $OCPartner, $OCstName, $OCstDept, $OCstDesn,   
        $OConName,   $OConEMail, $OTel,      $OFax,      $OEUPost,  $OEUAddr,  $OComments,
        $OChanges,   $OBusCat); 
    my  $parser    = Spreadsheet::ParseExcel::SaveParser->new();
    my  $workbook  = $parser->Parse($CMFile);
    my  $worksheet = $workbook->worksheet(0);

    my ($Row_min, $Row ) = $worksheet->row_range();
    my ($Col, $CRow, $Msg);
    if ($CustMod)
    {
        my ($SeqC,    $CustIDC,$BPartnerC,$SAgencyC,$EUserC, $DeptC,    $DesnC,
            $SAgentC, $SAMailC,$SATelC,   $SAFaxC,  $EUPostC,$EUAddrC,  $CommentsC,
            $ChangesC,$BusCatC,$RestC) = (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16);
        $CRow = $Customers{$OCstID}->{row};
        return (1, "Error: Could not Modify Cust! (Row not found)\n") unless ($CRow =~ /^\d+/ && ($CRow <= $Row)) ;
        {
             $Msg = "Updated $OCstID info \n";
             if ($ORPartner !~ /^\s*$/ && $ORPartner ne $Customers{$OCstID}->{bPartner})
             { 
                 my  $Cell = $worksheet->get_cell($CRow, $BPartnerC );
                 $worksheet->AddCell($CRow, $BPartnerC, $ORPartner);
                 $Msg .= "  BPartner From: $Customers{$OCstID}->{bPartner} \n".
                         "             To: $ORPartner\n";
             }
             if ($OCPartner !~ /^\s*$/ && $OCPartner ne $Customers{$OCstID}->{sAgency})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $SAgencyC );
                 $worksheet->AddCell($CRow, $SAgencyC, $OCPartner);
                 $Msg .= "  SAgent   From: $Customers{$OCstID}->{sAgency} \n".
                         "             To: $OCPartner\n";
             }
             if ($OCstName  !~ /^\s*$/ && $OCstName  ne $Customers{$OCstID}->{eUser})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $EUserC );
                 $worksheet->AddCell($CRow, $EUserC, $OCstName);
                 $Msg .= "  CstName  From: $Customers{$OCstID}->{eUser} \n".
                         "             To: $OCstName\n";
             }
             if ($OCstDept  !~ /^\s*$/ && $OCstDept  ne $Customers{$OCstID}->{eUDept})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $DeptC );
                 $worksheet->AddCell($CRow, $DeptC, $OCstDept);
                 $Msg .= "  CstDept  From: $Customers{$OCstID}->{eDept} \n".
                         "             To: $OCstDept\n";
             }
             if ($OCstDesn  !~ /^\s*$/ && $OCstDesn  ne $Customers{$OCstID}->{eUDesn})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $DesnC );
                 $worksheet->AddCell($CRow, $DesnC, $OCstDesn);
                 $Msg .= "  CstDesn  From: $Customers{$OCstID}->{eUDesn} \n".
                         "             To: $OCstDesn\n";
             }
             if ($OConName  !~ /^\s*$/ && $OConName  ne $Customers{$OCstID}->{sAgent})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $SAgentC );
                 $worksheet->AddCell($CRow, $SAgentC, $OConName);
                 $Msg .= "  AgName   From: $Customers{$OCstID}->{sAgent} \n".
                         "             To: $OConName\n";
             }
             if ($OConEMail !~ /^\s*$/ && $OConEMail ne $Customers{$OCstID}->{eUMail})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $SAMailC );
                 $worksheet->AddCell($CRow, $SAMailC, $OConEMail);
                 $Msg .= "  CstEMail From: $Customers{$OCstID}->{eUMail} \n".
                         "             To: $OConEMail\n";
             }
             if ($OTel      !~ /^\s*$/ && $OTel      ne $Customers{$OCstID}->{eUTel})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $SATelC );
                 $worksheet->AddCell($CRow, $SATelC, $OTel);
                 $Msg .= "  CstTel   From: $Customers{$OCstID}->{eUTel} \n".
                         "             To: $OTel\n";
             }
             if ($OFax      !~ /^\s*$/ && $OFax      ne $Customers{$OCstID}->{eUFax})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $SAFaxC );
                 $worksheet->AddCell($CRow, $SAFaxC, $OFax);
                 $Msg .= "  CstFax   From: $Customers{$OCstID}->{eUFax} \n".
                         "             To: $OFax\n";
             } 
             if ($OEUPost   !~ /^\s*$/ && $OEUPost   ne $Customers{$OCstID}->{eUPost})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $EUPostC );
                 $worksheet->AddCell($CRow, $EUPostC, $OEUPost);
                 $Msg .= "  CstPost  From: $Customers{$OCstID}->{eUPost} \n".
                         "             To: $OEUPost\n";
             }
             if ($OEUAddr   !~ /^\s*$/ && $OEUAddr   ne $Customers{$OCstID}->{eUAddr})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $EUAddrC );
                 $worksheet->AddCell($CRow, $EUAddrC, $OEUAddr);
                 $Msg .= "  CstAddr  From: $Customers{$OCstID}->{eUAddr} \n".
                         "             To: $OEUAddr\n";
             }
             if ($OComments !~ /^\s*$/ && $OComments ne $Customers{$OCstID}->{comments})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $CommentsC );
                 my  $NComments = $Customers{$OCstID}->{comments} . ",$OComments";
                 $worksheet->AddCell($CRow, $CommentsC, $NComments);
                 $Msg .= "  CstAddr  From: $Customers{$OCstID}->{comments} \n".
                         "             To: $NComments\n";
             }
             if ($OChanges !~ /^\s*$/ && $OChanges ne $Customers{$OCstID}->{changes})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $ChangesC );
                 my  $NChanges = $Customers{$OCstID}->{changes} . ",$OChanges";
                 $worksheet->AddCell($CRow, $ChangesC, $NChanges);
                 $Msg .= "  CstAddr  From: $Customers{$OCstID}->{changes} \n".
                         "             To: $NChanges\n";
             }
             if ($OBusCat   !~ /^\s*$/ && $OBusCat   ne $Customers{$OCstID}->{eUBusCat})
             {
                 my  $Cell = $worksheet->get_cell($CRow, $BusCatC );
                 $worksheet->AddCell($CRow, $BusCatC, $OBusCat);
                 $Msg .= "  BusCat   From: $Customers{$OCstID}->{eUBusCat} \n".
                         "             To: $OBusCat\n";
             }
        }
        
    }
    else
    {
        $Row ++;
        for $Col (0..$#CFlds)
        {
            my $Cell   = $worksheet->get_cell( ($Row - 1), $Col );
            my $FmtNo  = $Cell->{FormatNo};
            $worksheet->AddCell($Row, $Col, $CFlds[$Col], $FmtNo );
          # printf ("%2d %2d %s\n", $Row, $Col,$CFlds[$Col]); 
        }
        $Msg = "Added Cust $CustLastSeq, $OCstID $OCstName to CustomerMaster";
    }
    $workbook->SaveAs($CMFile);                           
    return (0, $Msg); 
}

sub GetSuppSeqs
{
    my  ($Res, $Msg, $TMsg, $SSeq) = (0, "", undef);
    my  ($PCKey, $OProdCodeO, $OProdCode);
    for  $PCKey  (sort keys %OProdCodes)
    {
         $OProdCodeO= $OProdCodes{$PCKey};
         $OProdCode = $OProdCodeO->{cProdCode};
         if   ($OProdCode =~ /WB0AX/       || $OProdCode =~ /MGA-1U/      || 
               $OProdCode =~ /BB0AX/       ||
               $OProdCode =~ /MG-01/       || $OProdCode =~ /NK4-MVLS-EX/ ||
                                              $OProdCode =~ /NK4-SMVLS-EX/||
               $OProdCode =~ /NK4-SES-0/   || $OProdCode =~ /NK-SES-0/    ||
               $OProdCode =~ /NK4-DSYN/    || $OProdCode =~ /NK4-EPCW/    ||
               $OProdCode =~ /NK4-SwiMon-SW-EX-01/                        ||
               $OProdCode =~ /NK4-ARPW-EX/ || $OProdCode =~ /NK4-W/       ||
               $OProdCode =~ /NK4-SMTP-01/ || $OProdCode =~ /CPMON-/      ||
               $OProdCode =~ /NK4-NMS/     || $OProdCode =~ /NK4-PCA/     ||
               $OProdCode =~ /NK4-QOLA-01/ || 
               $OProdCode =~ /CPTMON-CPT/  || $OProdCode =~ /NK4-IMS-01/  ||
               $OProdCode =~ /NK4-CON-01/  || $OProdCode =~ /NK4-KenMon/  ||
               $OProdCode =~ /NK4-RtrMon/  || $OProdCode =~ /NK4-CMA-1U/  ||
               $OProdCode =~ /NK4-CTA/     || $OProdCode =~ /MGW-01/      ||
               $OProdCode =~ /NK4-CVM/     || $OProdCode =~ /NK4-DCM/     ||
               $OProdCode =~ /NK4-FTS/     || $OProdCode =~ /NK4-IFBX/    ||
               $OProdCode =~ /NK4-ADM/     || $OProdCode =~ /NK4-MVLS-EX-LA/      ||
               $OProdCode =~ /NSV-/        || $OProdCode =~ /NK-ADM/      ||
               $OProdCode =~ /NK4-VLAG/    || $OProdCode =~ /NK-VLAG/     ||
               $OProdCode =~ /NK4-MGL/     ||
               $OProdCode =~ /NK4-RtrNCpMon/ )   # <== GLENN 
         {
              ($Res, $Msg, $SSeq) = &GetSuppSeq($PCKey);
              return ($Res, $Msg) if ($Res);
              $OProdCodeO->{sSeq} = $SSeq;
              $TMsg .= $Msg;
         }
         else
         {
              $OProdCodeO->{sSeq} = undef;
         }
    }
    return ($Res, $TMsg);
}

sub GetSuppSeq
{
    my  ($PCKey)    = @_;
    my  ($ProdCode) = $OProdCodes{$PCKey}->{cProdCode}; 
    my  ($CSeq, $CRec, $SSerial, $SPCode, $CPCode, $sheet, $mySheet,$SCnt,$PCnt, %LCounts);
    my   $parser    = Spreadsheet::ParseExcel->new();
    my  ($Res,$Msg, $SupF, $SupSheet) = &GetSupDBDets($PCKey);      # <--- Glenn-TBD
    return ($Res,$Msg) if ($Res); 
    my   $workbook  = $parser->Parse($SupF);
    my   $sheets    = $workbook->{SheetCount};
    for  $sheet (0..$sheets)
    {
        next unless  ($workbook->{Worksheet}[$sheet]->{Name} eq $SupSheet);
        $mySheet = $sheet;
        last;
    }
    if  (!defined $mySheet)
    {
         return (1, "Could not find sheet $SupSheet for $ProdCode in $SupF");
    }
    my  $worksheet = $workbook->worksheet($mySheet);
    my ($Row_min, $Row_max ) = $worksheet->row_range();
    my ($Row, $CCstID);
    my  $Msg  =  "GetSuppSeq: $ProdCode: $SupF, $SupSheet ";
    $SSeq     =   0;
    for $Row (5..$Row_max)
    {
        my ($Col_min, $Col_max)   = $worksheet->col_range();
        my  $NullRow = 1;
        my  $Col;
        if ($Row == 292)
        {
            print ("I have got the row $Row\n");
        }
        for $Col ($Col_min..$Col_max)
        {
            $NullRow = 0 if  (defined $worksheet->{Cells}[$Row][$Col] &&
                                      $worksheet->{Cells}[$Row][$Col]->Value =~ /\S+/);
            last         if  (! $NullRow);
        }
        next if ($NullRow);
       ($CSeq, $SCstID, $SSerial, $SPCode,$SCnt,$PCnt) = (0,undef,undef,undef,1,undef);
        $CSeq    = $worksheet->{Cells}[$Row][0]->Value  if (defined $worksheet->{Cells}[$Row][0]);
        $SCstID  = $worksheet->{Cells}[$Row][2]->Value  if (defined $worksheet->{Cells}[$Row][2]);
        $SPCode  = $worksheet->{Cells}[$Row][9]->Value  if (defined $worksheet->{Cells}[$Row][9]);
        $SSerial = $worksheet->{Cells}[$Row][10]->Value if (defined $worksheet->{Cells}[$Row][10]);
        $SCnt    = $worksheet->{Cells}[$Row][13]->Value if (defined $worksheet->{Cells}[$Row][13]);
        if ((! defined $SSerial || $SSerial =~ /^\s*$/) && $SPCode =~ /EX-LA/ )
        {
             $PCnt    = 0;
             $PCnt    = $LCounts{"$SCstID:$SPCode"} if (exists $LCounts{"$SCstID:$SPCode"});
             $SCnt   += $PCnt;
             $SSerial = sprintf "MVS-WB0AX-%04d-%04d", $PCnt + 1, $SCnt; 
             $LCounts{"$SCstID:$SPCode"} =  $SCnt; 
        }
        $SPCode  =~ s/NSK/NK4/;
        $SPCode  =~ s/NK5/NK4/;
        $SPCode  = &NormalPCode($SPCode,$ProdCode);
        $SPCode  = $CPCode if ($SPCode =~ /^\s*$/);
        $SSeq    = $CSeq   if ($CSeq   =~ /^\d+$/ && $CSeq > $SSeq);
        $SCstID  = $CCstID if ($SCstID =~ /^\s*$/ && defined $CCstID);
       #next if ($SPCode =~ /-LA$/);
        if  (($SSerial =~ /^\s*$/ && $SPCode =~ /^\s*$/)|| $SCstID =~ /^\s*$/) 
        {
              $Msg .= "Row=$Row: Illegal record CstID = $SCstID; Serial = $SSerial; $SPCode = $SPCode\n";
              next;
        }
        $SSerials{"$SCstID:$SPCode:$SSerial"} = $Row;
        $PSerials{        "$SPCode:$SSerial"} = $Row;
        $CustProd{"$SCstID:$SPCode"         } = $Row;
        $CCstID  = $SCstID;
        $CPCode  = $SPCode;
    }
    $SupSeqs{"$SupF $SupSheet"}->{sSeq} = $SSeq;                              
    return (0, "$Msg Got LastSupSeq $SSeq \n", $SSeq);
}


sub SysCopy
{
    my ($IFile, $OFile) = @_;
    copy($IFile, $OFile) || die "Could not copy $!";
}

sub GetLastArchIndex
{
    my ($TStamp) = @_;
    my  $LAInd   = 0;
    return $LAInd unless (-d $ARCD);
    die "Could not open $ARCD $!"
         unless opendir (DIR, $ARCD);
    my  @Files = grep { /\.csv/ } readdir DIR;
    my  $File;
    for $File (@Files)
    {
        next unless ($File =~ /$TStamp\_(\d+)\.csv/);
        $LAInd = $1 if ($LAInd < $1);
    }
    return ($LAInd);
}

sub FixExcels
{
    my  @Files     = ($NanoSuppFile);
    my  @FmtFields = (10);
    my ($File, $Index, $FileSv, $FmtField);
    for $Index ($#Files)
    {
        $File      = $Files[$Index];
        $FmtField  = $FmtFields[$Index];
        $FileSv    = $File;
        $FileSv    =~ s/\.xls$/\.BeforeFix\.xls/;
        move($File, $FileSv);
        &FixExcel($FileSv, $File, $FmtField );
    }
}

sub FixExcel
{
    my ($IFile, $OFile, $Col) = @_;
    system ("python FixExcels.py $IFile $OFile $Col");
}

sub FixExcelZaiko
{
    my  $Inv_NanoFO = $Inv_NanoF . ".$$";
    my ($IFile, $OFile, $Col) = ($Inv_NanoF, $Inv_NanoF, 1);
    system ("python FixExcels.py $IFile $OFile $Col");
    move ($OFile, $IFile);
}

sub FixExcelWB0AXCSupp
{
    my  $NanoSuppFileO = $NanoSuppFile . ".$$";
    my ($IFile, $OFile, $Col) = ($NanoSuppFile, $NanoSuppFileO, 10);
    system ("python FixExcels.py $IFile $OFile $Col");
    move ($OFile, $IFile);
}

sub FixExcelMGA1UCSupp
{
    my  $MGA1USuppFileO = $MGA1USuppFile . ".$$";
    my ($IFile, $OFile, $Col) = ($MGA1USuppFile, $MGA1USuppFileO, 10);
    system ("python FixExcels.py $IFile $OFile $Col");
    move ($OFile, $IFile);
}

sub FixExcelSupp
{
    my ($ProdCode) = @_;
    return (1, "Did not find details of ProdCode $ProdCode") 
            unless (exists $ProdToSupF{$ProdCode}); 
    my  $SupF     = $ProdToSupF{$ProdCode}->{pSupF}; 
    my  $SupSheet = $ProdToSupF{$ProdCode}->{pSupS};
    return (1, "SupFile specs not found  $ProdCode\n") unless (defined $SupF);
    $SupF         = $CSVD . $SupF . ".xls";
    return (1, "SupFile for $SupF does not exist for $ProdCode \n") unless (-f $SupF);
    my  $SupFO = $SupF . ".$$";
    my ($IFile, $OFile, $Col) = ($SupF, $SupFO, 10);
    system ("python FixExcels.py $IFile $OFile $Col");
    move ($OFile, $IFile);
    return (0, "Fixed SupFile for $ProdCode");
}

sub ArchiveFile
{
    my ($File) = @_;
    my  $FileA =  $ARCD . &ComLib::GetFileStub($File);
    $FileA     =~ s/\.xls$/\.$UpDtFTag\.xls/;
    SysCopy ($File, $FileA) if (-f $File);
}

sub SLog
{
    my  ($Msg) = @_;
    my   $LFile = "Dates.txt";
    die  "Could not open $LFile $!"
          unless open (LFILE, ">>$LFile");
    print LFILE $Msg;
    close LFILE;
}

sub PrintCustIDByBP
{
    my ($BP, $Msg);
    for $BP (sort keys %BPs)
    {
        $Msg .= sprintf  "%10s %10s\n", $BP, $BPs{$BP}
    }
    return (0, $Msg);
}

sub UpdtSuppXls
{
    my ($PCKey,$ProdCode,$Serials, $OldSerials, $UpdtLogF) = @_;
    my ($MRow, $Row, $Col, $SSeq, $SupStartR, $SupUpdate, $workbook, $worksheet, $sheet, $AddOrUpdt, $PHasSerial);
    $AddOrUpdt = "Added ";
    my ($Res, $Msg, $SupF, $SupSheet) = &GetSupDBDets($PCKey);
    return ($Res,$Msg) if ($Res); 
   #my  $SupF  = $MGA1USuppFile;
    my  $SupFO = $SupF;
    my ($RealRow,  $RealData, $mySheet) = (0,0,undef);
    my  $SupFAS= &GetFileStub($SupFO);
    my  $SupFA = $ARCD . $SupFAS;
    $SupFA     =~ s/\.xls/\.$UpDtFTag\.xls/;
    SysCopy ($SupF,  $SupFA) if (-f $SupF);
    my  $parser    = Spreadsheet::ParseExcel::SaveParser->new();
    my  $workbook  = $parser->Parse($SupF);
    my  $sheets    = $workbook->{SheetCount};
    for $sheet (0..$sheets)
    {
        next unless  ($workbook->{Worksheet}[$sheet]->{Name} eq $SupSheet);
        $mySheet = $sheet;
        last;
    }
    if  (!defined $mySheet)
    {
         return (1, "Could not find sheet $SupSheet for $ProdCode in $SupF");
    }
    my  $worksheet = $workbook->worksheet($mySheet);

    my ($AllowVern, $DelvVern) = ($OAllowVern, $ODelvVern);
    my  $OProdCodeO = $OProdCodes{$PCKey};
    $AllowVern = $OProdCodeO->{allowVern} if (exists $OProdCodeO->{allowVern} &&
                                                     $OProdCodeO->{allowVern} =~ /\S+/);
    $DelvVern  = $OProdCodeO->{delvVern}  if (exists $OProdCodeO->{delvVern}  &&
                                                     $OProdCodeO->{delvVern}  =~ /\S+/);
    $OLicCnt   = $OProdCodeO->{licCnt}    if (exists $OProdCodeO->{licCnt}    &&
                                                     $OProdCodeO->{licCnt}    =~ /\d+/);
    my  $NProdCode = $OProdCodeO->{cProdCode};
    $NProdCode     =~ s/-\d*MUPG//;
    $PHasSerial= 0;
    $PHasSerial= 1 if ($ProdToSupF{$NProdCode}->{pSerial} =~ /^S$/i);
    my ($SupConNo, $SupSDate, $SupEDate) = ("", "", "");
    $SupConNo  = $OSupConNo unless ($OSupConNo =~ /TBD/i);
    $SupSDate  = $OSupSDate unless ($OSupSDate =~ /TBD/i);
    $SupEDate  = $OSupEDate unless ($OSupEDate =~ /TBD/i);
    my  ($Serial,  $OldSerial )= (0, 0);
    my  (@Serials, @OldSerials, $CPSKey, $PSKey, $CPKey, $OCPSKey, $SProdCode);
    @Serials =  @$Serials; @OldSerials = @$OldSerials;
    my   $Ind;
    $Serials[0] = "ABCDEFGHIJKLMNOPQRSTUVWZYZ" if  ($#Serials < 0);
    for  $Ind (0..$#Serials)
    {
         my  @UpdtLog;
         my ($TAllowVern, $TDelvVern, $TCstName, $TCstDept, $TCstDesn, $TConName, $TLicCnt) = 
            ($AllowVern,  $DelvVern,  $OCstName, $OCstDept, $OCstDesn, $OConName, $OLicCnt) ; 
         ($SSeq,      $SBlank,    $SCstID,    $SRPartner,$SCPartner,$SCstName,   $SCstDept,
          $SCstDesn,  $SConName,  $SProdCode, $SSerial,  $SAcct,    $SPass,      $SLicCnt,
          $SDelvDate, $SUseSDate, $SAllowVern,$SDelvVern,$SSupConNo,$SSupSDate,  $SSupEDate,
          $SComments)= 
         (undef,     undef,      undef,      undef,     undef,     undef,       undef,
          undef,     undef,      undef,      undef,     undef,     undef,       undef,
          undef,     undef,      undef,      undef,     undef,     undef,       undef,
          undef);
         $Serial    = $Serials[$Ind];
         $Serial    = undef if ($Serial eq "ABCDEFGHIJKLMNOPQRSTUVWZYZ");
         $OldSerial = $OldSerials[$Ind];
         $OCPSKey   = undef;
         $OCPSKey   = "$OCstID:$NProdCode:$OldSerial" if (defined $OldSerial);
         if  (defined $OCPSKey && exists $SSerials{$OCPSKey})
         {
              $Row = $SSerials{$OCPSKey};
              my $Cell   = $worksheet->get_cell($Row, 21 );    # The comments row
              my $Com    = $Cell->{_Value} . " -> $Serial ($CPRegDateS) ";
              $worksheet->AddCell($Row, 21, $Com);
              $SupUpdate = 1;
         }
 
         $CPSKey    = "$OCstID:$NProdCode:$Serial";
         $PSKey     =         "$NProdCode:$Serial";
         $CPKey     = "$OCstID:$NProdCode";
         $Row = 0;
         $Row = $SSerials{$CPSKey} if  (exists $SSerials{$CPSKey});
        #$Row = $PSerials{$PSKey } if  (exists $PSerials{$PSKey } && $Row == 0 && $Serial =~ /\S+/);
         $Row = $CustProd{$CPKey } if  (exists $CustProd{$CPKey } && $Row == 0 && !$PHasSerial);
        #$Row = $CSerials{$CSKey } if  (exists $CSerials{$CSKey } && $Row == 0 && $PHasSerial );
         if  ($Row)
         {
              my (@PSFlds);
              $AddOrUpdt = "Updated ";
              for  $Col (0..($SupFlds - 1))
              {
                   my   $Cell = $worksheet->get_cell($Row, $Col );
                   if  (!defined $Cell)
                   {
                         print "Undefined cell Row = $Row, Col = $Col \n";
                         next;
                   }
                   $PSFlds[$Col] = $Cell->{_Value}; 
              }
             ($SSeq,      $SBlank,    $SCstID,    $SRPartner,$SCPartner,$SCstName,   $SCstDept,
              $SCstDesn,  $SConName,  $SProdCode, $SSerial,  $SAcct,    $SPass,      $SLicCnt,
              $SDelvDate, $SUseSDate, $SAllowVern,$SDelvVern,$SSupConNo,$SSupSDate,  $SSupEDate,
              $SComments) = @PSFlds;
              $SupUpdate = 1;
         }
         else
         {
              my  $ProdCodeO = $OProdCodes{$PCKey};
              $SupSeqs{"$SupF $SupSheet"}->{sSeq} = 0 if (! defined $SupSeqs{"$SupF $SupSheet"}->{sSeq});
              $SupSeqs{"$SupF $SupSheet"}->{sSeq} ++;
              $SSeq      = $SupSeqs{"$SupF $SupSheet"}->{sSeq};
              $SProdCode = $NProdCode;
              $SupUpdate = 0;
         }
         $TAllowVern = $SAllowVern unless ($TAllowVern =~ /\S+/);
         $TDelvVern  = $SDelvVern  unless ($TDelvVern  =~ /\S+/);
         $TCstName   = $SCstName   unless ($TCstName   =~ /\S+/);
         $TCstDept   = $SCstDept   unless ($TCstDept   =~ /\S+/);
         $TCstDesn   = $SCstDesn   unless ($TCstDesn   =~ /\S+/); 
         $TConName   = $SConName   unless ($TConName   =~ /\S+/); 
         $TLicCnt    = $SLicCnt    unless ($TLicCnt    =~ /\S+/); 
          my  @SFlds  =
            ($SSeq,       $SBlank,    $OCstID,    $ORPartner,  $OCPartner, $TCstName,   $TCstDept,
             $TCstDesn,   $TConName,  $SProdCode, $Serial,     $SAcct,     $SPass,      $TLicCnt,
             $ODelvDate,  $SUseSDate, $TAllowVern,$TDelvVern,  $SupConNo,  $SupSDate,   $SupEDate,
             $OComments,  $ORegnDate);
     
         if (!$Row)
         {
             my ($Row_min, $Row_max ) = $worksheet->row_range();
             for $MRow (5..$Row_max)
             {
                 my $Cell0  = $worksheet->get_cell($MRow, 0 );
                 my $Cell2  = $worksheet->get_cell($MRow, 2 );
                 $Row       = $MRow if ($Cell0->{_Value} =~ /\S+/ || $Cell2->{_Value} =~ /\S+/);
             }
             $Row = 4 unless ($Row >= 5);
             $Row ++;
         }
         my  $Comments   = $OComments;
         $Comments       =~ s/OLD:(\S+)\s*//;
         $Comments       = "$OldSerial -> ($CPRegDateS) " if (defined $OldSerial);
         $Serial        .= " -> ($CPRegDateS) "           if (defined $OldSerial && $Serial eq $OldSerial);
         $SFlds[10]      = $Serial                        if (defined $OldSerial && $Serial eq $OldSerial);                       
         $SFlds[21]      = $Comments;
         push @UpdtLog , &ComLib::GetFileStub($SupF);
         push @UpdtLog , $SupSheet;
         push @UpdtLog , $OProdCodeO->{prodCode};        
         push @UpdtLog , $AddOrUpdt;        
         push @UpdtLog , $OConEMail;
         push @UpdtLog , $Row;
         for $Col (0..$#SFlds)
         {
             my $Cell   = $worksheet->get_cell(5, $Col ); #Row No. 6 is the right one!
             my $FmtNo  = $Cell->{FormatNo};
             if  (($SupUpdate) && ($Col == $SupSDateCol)) # Do not update Support Start date for Support Updates
             {
                 my $Cell1     = $worksheet->get_cell($Row,$SupSDateCol);
                 my $SupSDate  = $Cell1->{_Value};
                 push @UpdtLog , $SupSDate if ($SupSDate =~ /\S+/);
                 next if ($SupSDate =~ /\S+/);
             }
             if  (($SupUpdate) && ($Col == $DelvDateCol)) # Do not update Delivery date for Support Updates
             {
                 my $Cell1     = $worksheet->get_cell($Row,$DelvDateCol);
                 my $DelvDate  = $Cell1->{_Value};
                 push @UpdtLog , $DelvDate if ($DelvDate =~ /\S+/);
                 next if ($DelvDate =~ /\S+/);
             }
             $worksheet->AddCell($Row, $Col, $SFlds[$Col], $FmtNo);
             push @UpdtLog , $SFlds[$Col];
         }
         &LogUpdtLog($UpdtLogF, \@UpdtLog);
    }
    $workbook->SaveAs($SupF); 
    return (0, "$AddOrUpdt Suprec: $SSeq CustProd $OCstID $PCKey $Serial $OSupConNo, $OSupSDate,  $OSupEDate to Support");
}

sub GetSupDBDets
{
    my  ($PCKey) = @_;

    my  ($Res, $Msg, $SupFile, $SupSheet) = (0,"",undef, undef);
    my  (@ProdSerials) = split ',',  $OProdCodes{$PCKey}->{serials};     # <==== NEW
    my   $ProdSerial   = $ProdSerials[0];
    my   $ProdCode     = $OProdCodes{$PCKey}->{cProdCode};
    $ProdCode =~ s/-\d*MUPG// if ($ProdCode =~ /-\d*MUPG/);
    if (($ProdCode =~ /NK4-MVLS-EX-LA/) &&
       (($ProdSerial =~ /^MVS-WB0AX/) || ($ProdSerial =~ /^MVS-BB0AX/) 
                                      || ($ProdSerial =~ /^MVLS-EX/  )))   # Guess MVLS license sheet! New License numbering
    {
         $ProdCode = "NK4-MVS-WB0AX"  if ($ProdSerial =~ /^MVS-WB0AX/);  # <==== NEW
         $ProdCode = "NSK-MVS-BB0AX"  if ($ProdSerial =~ /^MVS-BB0AX/);  # <==== NEW
         $ProdCode = "NK4-MVLS-EX-LA" if ($ProdSerial =~ /^MVLS-EX/);    # <==== NEW
    }
    elsif  ($ProdCode =~ /NK4-MVLS-EX-LA/)   # Guess MVLS license sheet!
    {
         my   $PCKey;
         for  $PCKey  (sort keys %OProdCodes)
         {
              my $OProdCodeO  = $OProdCodes{$PCKey};
              my $OProdCode   = $OProdCodeO->{prodCode};
              if (($OProdCode =~ /NK4-MVLS-EX/ || 
                   $OProdCode =~ /NK4-MVS-WB0AX/   ) && $OProdCode !~ /NK4-MVLS-EX-LA/) 
              {
                   $ProdCode  = $OProdCode;
                   $ProdCode  =~ s/-\d*MUPG//    if ($ProdCode =~ /-\d*MUPG/);
                   last;
              }
         }
    }
    return (1, "Did not find details of ProdCode $ProdCode") 
           unless (exists $ProdToSupF{$ProdCode}); 
    $SupFile  = $ProdToSupF{$ProdCode}->{pSupF}; 
    $SupSheet = $ProdToSupF{$ProdCode}->{pSupS};
    $SupFile  = $CSVD . $SupFile . ".xls" if (defined $SupFile);
    return ($Res, $Msg, $SupFile, $SupSheet);
}

sub LoadProdToSupDets
{
    my  ($RecNo, $ProdCodes, $PRODTOSUPF, $CRec) = (0, 0, undef, undef);
    my  (%SupFs);
    return (1, "no file $ProdToSupF in LoadProdToSupDets ") unless (-f $ProdToSupF);
    die "Could not open $ProdToSupF $!"
         unless open ($PRODTOSUPF, "< :encoding($EncodingI)", $ProdToSupF);
    while ( $CRec = $CsvF->getline($PRODTOSUPF) )
    {
        $RecNo++;
        my ($ProdCode, $ProdName, $SupF, $Dummy, $SSheet, $Purpose, $Items, $Serial, $Other) = @$CRec;
        next if ($ProdCode =~ /^\s*#\s*/ || ($ProdCode !~/\s*NK4/ && $ProdCode !~/\s*NK-SES-/ && $ProdCode !~ /NSV-/
                                                                  && $ProdCode !~/\s*CPMON-/  && $ProdCode !~ /NK-ADM/
                                                                  && $ProdCode !~/\s*NK5/     && $ProdCode !~ /\s*NSK/
                                                                  && $ProdCode !~/\s*NK-VLAG/));
        $ProdCode =~ s/\-\d*MUPG//;
        next if ($ProdCode =~ /^\s*$/ ); 
        next if ($SupF     =~ /^\s*$/ );
        if (exists $ProdToSupF{$ProdCode})
        {
            my $ORecNo = $ProdToSupF{$ProdCode}->{pRec};
          # print "Duplicate entry for $ProdCode : $RecNo, $ORecNo \n" ;  
            &Log ("Duplicate entry for $ProdCode : $RecNo, $ORecNo \n");  
            next;
        } 
        $ProdCodes++;
        $ProdToSupF{$ProdCode}->{pSupF}   = $SupF;
        $ProdToSupF{$ProdCode}->{pSupS}   = $SSheet;
        $ProdToSupF{$ProdCode}->{pName}   = $ProdName;
        $ProdToSupF{$ProdCode}->{pRec }   = $RecNo;
        $ProdToSupF{$ProdCode}->{pSerial} = $Serial;
        next if (exists $SupFs{$SupF});
        push @SupFs, $SupF; 
        $SupFs{$SupF} = 1;
    }
    close $PRODTOSUPF;
    return (0, "Loaded $ProdCodes ProdCodes $#SupFs");
}

sub PrintProdToSupDets
{
    my  ($ProdCode, $ProdCodes, $ProdToSupFO) = (0, 0, undef);
    my  ($SupF);
    for  $ProdCode (sort keys   %ProdToSupF)
    {
         $ProdCodes++;
         $ProdToSupFO = $ProdToSupF{$ProdCode};
        #printf "%30s %30s %10s \n",$ProdCode,
        #        $ProdToSupFO->{pSupF}, $ProdToSupFO->{pSupS};
         printf "%25s %20s %s \n",$ProdCode,
                 $ProdToSupFO->{pSupS}, $ProdToSupFO->{pSupF};
    }
    print "Totally $ProdCodes prodcodes printed \n";
    for $SupF (@SupFs)
    {
        print  $SupF, "\n";
    }
    print "Totally $#SupFs Support files printed \n";
}

sub GetFileStub
{
    my ($FName) = @_;
    my  $Pos    = rindex $FName, '/';
    my  $FStub  = $FName;
    $FStub  = substr $FName, ($Pos+1) if ($Pos > 0);
    return $FStub;
}

sub NormalPCode
{
    my  ($PCode, $OProdCode) = @_;
    my   $NCode  = $PCode;
    $NCode = "NK4-MG-01"         if ($PCode =~ /\(Win\)/   && $OProdCode =~ /NK4-MG-01-1MUPG/ );
    $NCode = "NK4-MG-01"         if ($PCode =~ /\(Linux\)/ && $OProdCode =~ /NK4-MG-01-1MUPG/ );
    $NCode = "NK4-ARPW-EX"       if ($PCode eq "NK-ARPW-01"&& $OProdCode =~ /NK4-ARPW-EX-1MUPG/ );
    $NCode = "NK4-KenMon-SW-01"  if ($PCode eq "NK-KMON-01");
    return $NCode; 
}

sub LogUpdtLog
{
    my ($UpdtLogF, $UpdtLog) = @_;
    my  $UPDTLOGF;
    die "Could not open $UpdtLogF $!"
         unless open ($UPDTLOGF, ">> :encoding($EncodingI)",$UpdtLogF);
    $CsvF->print($UPDTLOGF, $UpdtLog);
    close $UPDTLOGF;
}

sub QuerySuppXls
{
    my ($Cust,$SupF, $SupS, $RespLogF, $SkipReplacedItems) = @_;
    my ($MRow, $Row, $Col, $SSeq, $SupStartR, $SupUpdate, $workbook, $worksheet, $sheet);
    my (%ProdKeys, %CustCatSers, $Dups, $Cons);
    my  $SupFP = $DDIR . $SupF . ".xls";
    return (1, 0,0, "Did not $SupFP") unless (-f $SupFP);
    my ($Hits, $Rows, $ECnt, $Msg) = (0,0,0,"");
    my ($mySheet, $AddOrUpdt) = (undef, "QUERY");
    my  $parser    = Spreadsheet::ParseExcel->new();
    my  $workbook  = $parser->Parse($SupFP);
    my  $sheets    = $workbook->{SheetCount};
    my  $SupFL     = "tmpLog/$SupF";
   #For New Excel
    my  $RespXLF   = $RespLogF;
    $RespXLF       =~ s/\.csv/\.xls/;
    my  $XLogF     = $RespLogF;
    $XLogF         =~ s/\.csv/\.log/;
    SysCopy ($DDIR . "Support_NK4_templatefile.xls" , $RespXLF);
    my  $parser2   = Spreadsheet::ParseExcel::SaveParser->new();
    my  $workbook2 = $parser2->Parse($RespXLF);
    my  $worksheet2= $workbook2->worksheet(0);
    my ($Row_min2, $Row2 ) = $worksheet2->row_range();
    $worksheet2->{Name}    = $SupS;
    $Row2 = 5;
    
    
    ($Dups, $Cons) = (0,0);
    unlink  $SupFL;
    for $sheet (0..$sheets)
    {
        next unless  ($workbook->{Worksheet}[$sheet]->{Name} eq $SupS);
        $mySheet = $sheet;
        last;
    }
    if  (!defined $mySheet)
    {
         return (1, $Hits, $Rows, "Could not find sheet $SupS for $Cust in $SupF");
    }
    my  $worksheet = $workbook->worksheet($mySheet);
    my ($Col_min, $Col_max)   = $worksheet->col_range();
    my ($Row_min, $Row_max)   = $worksheet->row_range();
    my ($Val, $BadC, $BadD)          = (undef, "\x{ff0d}", "\x{fffd}"); #Unmappable chars !
    for $Row (1..$Row_max)
    {
        my (@RespLog, $CustCatSerSup, $CustCatSer, $ConMsg,$Dup) = (undef,undef,undef,"",0);
        my ($SDate, $EDate, $NDate, $NDateKey);
        my ($Cst, $Ser, $Lic, $Sup);
        my  $Cell   = $worksheet->get_cell($Row, 2 ); #Col 2 is CustNo
        next if (($Cell->{_Value} !~ /^CK/) || (defined $Cust && $Cell->{_Value} ne $Cust));
        $Cell   = $worksheet->get_cell($Row, 10 );   #Col 10 is SerialNo
        next if (($Cell->{_Value} =~ /\-\>/) && ($SkipReplacedItems));
       #if   ($Cell->{_Value} eq "CK0010025")
       #{
       #      print "Got the first line ", $Cell->{_Value}, "\n"; 
       #}
        push @RespLog , &ComLib::GetFileStub($SupF);
        push @RespLog , $SupS;
        push @RespLog , "PROD";
        push @RespLog , $AddOrUpdt;        
        push @RespLog , $Row;
        for  $Col (0..$Col_max)
        {
             $Cell   = $worksheet->get_cell($Row, $Col );
             $Val    = $Cell->{_Value};
             $Val    =~ s/$BadC//g if (defined $Val);
             $Val    =~ s/$BadD//g if (defined $Val);
             $Val    =  &GetNormalizedDate($Val) if ($Col == 14 || $Col == 19 || $Col == 20); # Fix the dates
             if   ($Col == 15)
             {
                   if (($Val =~ /\d+/ ) && (length ($Val) < 6 ))
                   {
                       &libLog($SupFL, sprintf ("%4d Illegal Serial $Val Row: %5d New:%06d\n", $ECnt++, $Row, $Val)); 

                   }
                   elsif ($Val =~ /\-\>/)
                   {
                       &libLog($SupFL, sprintf ("$Row: %4d Replaced Serial $Val \n"), $ECnt++); 
                   }
             }
             if  ($Col == 7 && $Val == 0)
             {
                  $Val = ""; &libLog($SupFL, sprintf ("$Row: %4d Replaced $Val by blank in Shozoku \n"), $ECnt++);
             }
             push @RespLog , $Val;
             $Cst = $Val if ($Col ==  2); $Ser = $Val if ($Col == 10); 
             $Lic = $Val if ($Col == 13); $Sup = $Val if ($Col == 18); 
             if   ($Col == 2 || $Col == 9 || $Col == 10 || $Col == 13 || $Col == 18 )
             {
                  $CustCatSerSup .= ($Val . " ");
                  $CustCatSer    .= ($Val . " ") unless ($Col == 18);

             }
             if ($Col == 19 || $Col == 20)
             {
                 if   ($Col == 20)
                 {
                       $EDate = $Val;
                       $NDate = &GetNDate($Val);
                       $CustCatSer .=  $NDate;
                       $CustCatSers{$CustCatSer}->{sDate} = $SDate;
                       $CustCatSers{$CustCatSer}->{eDate} = $EDate;
                       $CustCatSers{$CustCatSer}->{rec}   = $Row;
                       $CustCatSers{$CustCatSer}->{nDate} = $NDate;
                       $CustCatSers{$CustCatSer}->{row}   = $Row;
                 }
                 else
                 {
                       $SDate = $Val;
                       $NDateKey = $CustCatSer . $SDate;
                       if   ( $SDate =~ /\S+/ && exists  $CustCatSers{$NDateKey})
                       {
                              $ConMsg = ("Con: Prv:" .   $CustCatSers{$NDateKey}->{row} .   " Cur: ". $Row . 
                                    " ". $NDateKey . 
                                    " ". $CustCatSers{$NDateKey}->{sDate} . " " . $Val);
                              $Cons++;
                       }
                 }
             }
        }
       #my ($SuppS, $SuppE) = &SanitizeSuppDates(\@RespLog); 
        my ($DelvD,$SuppS, $SuppE) = &SanitizeSuppDatesF(\@RespLog); #DateFormat fix
        if   ($Cst eq "CK0010209" && $Lic == 5 )     #  && $Ser == 707006225 )
        {
               &libLog($XLogF, "$Row $CustCatSer ; Got Cst $Cst with Lic = $Lic  D= $DelvD,S= $SuppS, E = $SuppE ". 
                "RS= ". $CustCatSers{$CustCatSer}->{sDate}. " RE=". $CustCatSers{$CustCatSer}->{eDate}. " N=". $CustCatSers{$CustCatSer}->{nDate}. "\n");
        }
        ($RespLog[24], $RespLog[29], $RespLog[30]) = ($DelvD, $SuppS, $SuppE);
        push @RespLog, $ConMsg if ($ConMsg =~ /\S+/);
        if  (exists $ProdKeys{$CustCatSerSup})
        {
             my ($Ind, $Match) = (undef,1);
             my  @ERespLog = @{$ProdKeys{$CustCatSerSup}->{flds}};
             $Dup = 1;
             for $Ind (12..30)
             {
                 if  (($Ind == $DelvDateCol + 10 && (($ERespLog[$Ind] =~ /^\s*$/) || ($ERespLog[$Ind] =~ /^\s*$/ ))) ||
                      ($Ind == $AccountCol  + 10 && (($ERespLog[$Ind] =~ /^\s*$/) || ($ERespLog[$Ind] =~ /^\s*$/ ))) ||
                      ($Ind == $PasswdCol   + 10 && (($ERespLog[$Ind] =~ /^\s*$/) || ($ERespLog[$Ind] =~ /^\s*$/ )))    )
                 {
                      &libLog($XLogF, "$Row: $Ind $ERespLog[$Ind] $RespLog[$Ind] mismatch Ignored \n");
                      next;
                 }
                #if  ($Ind == $DelvDateCol + 10 && #The catalog number case!
                 if  ($ERespLog[$Ind] ne $RespLog[$Ind] )
                 {
                      &libLog($XLogF, "$Row: $Ind $ERespLog[$Ind] $RespLog[$Ind] mismatched\n");
                      $Dup = 0;
                      last;
                 }
             }
             push @RespLog, "Dup: " . $ProdKeys{$CustCatSerSup}->{rec}. " ". $Row . " ". $CustCatSerSup if ($Dup);
        }
        else
        {
             $ProdKeys{$CustCatSerSup}->{rec} = $Row;
             $ProdKeys{$CustCatSerSup}->{flds}= \@RespLog;
        }
        $Dups++    if ($Dup);
        ### For new excel
        if  (!$Dup && ($ConMsg !~ /\S+/))
        {
             for  $Col (0..($SupFlds - 1))
             {
                  my $Cell   = $worksheet2->get_cell( (6 - 1), $Col );
                  my $FmtNo  = $Cell->{FormatNo};
                  $worksheet2->AddCell($Row2, $Col, $RespLog[$Col+10], $FmtNo );
                 #print "Added $Row2, $Col, $RespLog[$Col+5], $FmtNo\n" 1887;
             }
             $worksheet2->AddCell($Row2, $SupFlds, $Customers{$Cst}->{eUBusCat});
             if   ($Cst eq "CK0010209" && $Lic == 5 )
             {
                   &libLog($XLogF, "Assigned $CustCatSer nRow $Row2\n");
             }
             $CustCatSers{$CustCatSer}->{nRow}   = $Row2;
             $Row2++;
        }
        else
        {

             if   ($Cst eq "CK0010209" && $Lic == 5 )
             {
                   &libLog($XLogF, "Skipped $CustCatSer Sup = $Dup CMsg = $ConMsg\n");
             }
        }
        if  ($ConMsg =~ /\S+/)
        {
             #Fix the earlier record!
             my  ($ORow,$OCol) = ($CustCatSers{$NDateKey}->{nRow},0);
             for  $OCol (1..($SupFlds - 1)) # Do not change Row Seq 
             {
                  next if ($OCol == $SupSDateCol || $OCol == $DelvDateCol);
                  my $Cell   = $worksheet2->get_cell( (6 - 1), $OCol );
                  my $FmtNo  = $Cell->{FormatNo};
                  my $OCell  = $worksheet2->get_cell( $ORow, $OCol );
                  my $OVal   = $OCell->{_Value};
                     $OVal   = sprintf ("%06d", $OVal) if  ($OCol == 10 && $OVal =~ /^\d\d\d\d\d\$/);
                     $OVal  .= " $RespLog[$OCol+10]"   if  ($OCol == $SupCommCol);
                  $worksheet2->AddCell($ORow, $OCol, $RespLog[$OCol+10], $FmtNo ) unless ($OCol == 10 || $OCol == $SupCommCol);
                  $worksheet2->AddCell($ORow, $OCol, $OVal             , $FmtNo ) if     ($OCol == 10 || $OCol == $SupCommCol);
                  &libLog($XLogF, "$Cst Updated $ORow ($Row), $OCol from $OVal, to $RespLog[$OCol+10] \n")
                                                   if   ($OCol ==  $SupEDateCol || $OCol == $SupConNoCol );
             }
             $CustCatSers{$CustCatSer}->{nRow}   = $ORow;
             delete ($CustCatSers{$NDateKey});
        }
        $Hits++;
        &LogUpdtLog($RespLogF, \@RespLog);
    }
    $workbook2->SaveAs($RespXLF);
    my  $RespXLFO = $RespXLF . ".$$";
    my ($IFile, $OFile, $Col) = ($RespXLF, $RespXLFO, 10);
    system ("python FixExcels.py $IFile $OFile $Col");
    move ($OFile, $IFile);
    return (0, $Hits, $Row_max, "$Hits Recs in $SupF:$SupS (Dups=$Dups, Cons=$Cons)");
}

sub GetNDate
{
    my ($Date) = @_;
    my ($NDate, $Y, $M, $D, $h, $m, $s);
   ($Y, $M, $D) = ($1, $2, $3) if ($Date =~ /^(\d+)\/(\d+)\/(\d+)$/);
   ($Y, $M, $D) = ($1, $2, $3) if ($Date =~ /^(\d+)-(\d+)-(\d+)$/);
   ($h, $m, $s) = (0,0,0);
    $M -=1;
    $Y  = ($Y - 1900) if ($Y < 2000);
    my  $UTime  = timelocal_nocheck($s, $m, $h, $D, $M, $Y);
    $UTime += 86400;
    ($s, $m, $h, $D, $M, $Y, $wday,$yday,$isdst) = localtime($UTime);
    $NDate = sprintf ("%02d/%02d/%02d", $Y + 1900, $M + 1, $D);
    return ($NDate);
}


sub SanitizeSuppDatesF
{
    my ($SuppFldsP) = @_;
    my  @SuppFlds   = @$SuppFldsP;
    my ($DelvD, $SuppS, $SuppE) = ($SuppFlds[24],$SuppFlds[29], $SuppFlds[30]);
    $DelvD = &GetNormalizedDate($DelvD);
    $SuppS = &GetNormalizedDate($SuppS);
    $SuppE = &GetNormalizedDate($SuppE);
    return ($DelvD, $SuppS, $SuppE);
}

sub GetNormalizedDate
{
    my ($IDate) = @_;
    my  $NDate  = $IDate;
    return $NDate unless ($NDate =~ /\S+/);
    my ($NDate);
    $NDate = sprintf ("%d/%02d/%02d", $1,$2,$3) if ($IDate =~ /(\d+)-(\d+)-(\d+)$/   && $1 > 1900 & $2 <=12);
    $NDate = sprintf ("%d/%02d/%02d", $3,$2,$1) if ($IDate =~ /(\d+)-(\d+)-(\d+)$/   && $3 > 1900 & $2 <=12);
    $NDate = sprintf ("%d/%02d/%02d", $1,$2,$3) if ($IDate =~ /(\d+)\/(\d+)\/(\d+)$/ && $1 > 1900 & $2 <=12);
    $NDate = sprintf ("%d/%02d/%02d", $3,$2,$1) if ($IDate =~ /(\d+)\/(\d+)\/(\d+)$/ && $3 > 1900 & $2 <=12);
    $NDate = &ConvExDate($IDate) if ($IDate =~ /^\d+$/);
    return $NDate;
}

sub ConvExDate
{
    my ($XDate) = @_;
    my  $Time   = ($XDate - 25569) * 24 * 3600;
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($Time);
    my $NDate   = sprintf ("%d/%02d/%02d", $year + 1900,$mon + 1,$mday);
    return $NDate;
}

sub SanitizeSuppDates
{
    my ($SuppFldsP) = @_;
    my  @SuppFlds   = @$SuppFldsP;
    my ($Prod, $CustID, $SuppS, $SuppE) = ($SuppFlds[2], $SuppFlds[7], $SuppFlds[24], $SuppFlds[25]);
         if  (! defined $SuppS || ($SuppS =~ /^\s*$/)||
              ! defined $SuppS || ($SuppS =~ /^\s*$/)  )
         {
              if    ($CustID =~ /CK034/ && 
                    ($Prod =~ /CSE_WB0AX/ ||  $Prod =~ /CSE_WB0AX/ ||
                     $Prod =~ /CSE_BB0AX/ ||  $Prod =~ /CSE_BB0AX/ ) &&
                     (1) )    # Count check! $CSE_WB0AX > 0)
              {
                     $SuppS = "2010/02/02";
                     $SuppE = "2030/02/02";
              }
              elsif ($CustID =~ /CK001/)
              {
                     $SuppS = "2010/01/01" if ($SuppS =~ /^\s*$/);
                     $SuppE = "2010/01/01" if ($SuppE =~ /^\s*$/);
              }
              else
              {
                     $SuppS = "2020/02/02" if ($SuppS =~ /^\s*$/);
                     $SuppE = "2020/02/02" if ($SuppE =~ /^\s*$/);
              }
         }
   return ($SuppS, $SuppE);
}

sub GetSupLost 
{
    my ($Cust,$SupF, $SupS, %SupLosts);
    my ($MRow, $Row, $Col, $SSeq, $SupStartR, $SupUpdate, $workbook, $worksheet, $sheet);
   ($SupF, $SupS)  = ($SupStatusF, "SupStatus");
    return (1, 0,0, undef, "Did not find $SupF") unless (-f $SupF);
    my ($Hits, $Rows, $Msg) = (0,0,"");
    my ($mySheet ) = (undef);
    my  $parser    = Spreadsheet::ParseExcel->new();
    my  $workbook  = $parser->Parse($SupF);
    my  $sheets    = $workbook->{SheetCount};
    for $sheet (0..$sheets)
    {
        next unless  ($workbook->{Worksheet}[$sheet]->{Name} eq $SupS);
        $mySheet = $sheet;
        last;
    }
    if  (!defined $mySheet)
    {
         return (1, $Hits, $Rows, undef, "Could not find sheet $SupS in $SupF");
    }
    my  $worksheet = $workbook->worksheet($mySheet);
    my ($Col_min, $Col_max)   = $worksheet->col_range();
    my ($Row_min, $Row_max)   = $worksheet->row_range();
    my ($Val)     = (undef);
    for $Row (1..$Row_max)
    {
        my @RespLog;
        my $Cell   = $worksheet->get_cell($Row, 10 ); #Col 10 is Support active/dead
        my $Status = $Cell->{_Value};
        $Status    =~ s/\s*//g;
        next unless ($Status eq "\x{4E0D}\x{66F4}\x{65B0}");
        $Cell      = $worksheet->get_cell($Row, 2 );  #Col  2 is CustNo
        $Cust      = $Cell->{_Value};
        $Cell      = $worksheet->get_cell($Row, 5 );  #Col  5 is SuppNo
        $SupLosts{$Cust}->{suppC} = $Cell->{_Value};
        $Hits++;
    }
    return (0, $Hits, $Row_max, \%SupLosts, "$Hits LostSupports in $SupF");
}

sub libLog
{
    my ($SupFL, $Msg) = @_;
    die "Could not open $SupFL $!"
         unless open (LOGF, ">>$SupFL");
    print LOGF $Msg;
    close LOGF;
}
1;
