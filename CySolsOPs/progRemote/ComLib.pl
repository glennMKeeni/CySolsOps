#!/usr/bin/perl
# V-1.3 20130915 200000
package ComLib;
use strict;
use Time::Local 'timelocal_nocheck';
our (%Log);

#
# Get File MTime
#
sub GetFTime
{
    my  ($File) = shift;
    return undef unless (-f $File);
    my  ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size, $atime,
                   $mtime,$ctime,$blksize,$blocks) = stat($File);
    return $mtime;
}

sub GetFileStub
{
    my ($FName) = @_;
    my  $Pos    = rindex $FName, '/';
    my  $FStub  = $FName;
    $FStub  = substr $FName, ($Pos+1) if ($Pos > 0);
    return $FStub;
}

sub GetNameFromFP
{
    my ($FP)  = @_;
    my $PName = &GetFileStub($FP);
    $PName    =~ s/\.\S+$//;
    return $PName;
}

#
# Do the Splitting with care!
#
sub SplitCSVLine {
    my ($tmp) = @_;
    $tmp =~ s/(?:\x0D\x0A|[\x0D\x0A])?$/,/;
    my @values =
       map { /^"(.*)"$/ ? scalar( $_ = $1, s/""/"/g, $_ ) : $_ }
       ( $tmp =~ /("[^"]*(?:""[^"]*)*"|[^,]*),/g );
    return @values;
}

sub MkDirs
{
    my  (@Comps) = @_;
    my  ($Comp, $Dir);
    for  $Comp (@Comps)
    {
         $Dir .= $Comp . "/";
         mkdir $Dir unless (-d $Dir);
    }
}

sub GetVersion
{
    my ($VerF) = @_;
    my ($VerNumber, $VerBuild, $VerModel) = (undef, undef, undef);
    my  $RecNo = 0;
    return ($VerNumber, $VerBuild, $VerModel) unless (-f $VerF);
    die  "Could not open $VerF $!"
          unless open (VERF, $VerF);
    while (<VERF>)
    {
           chop ;
           next if (/^\s*$/);
           $RecNo++;
           $VerNumber = $_ if ($RecNo == 1);
           $VerBuild  = $_ if ($RecNo == 2);
           $VerModel  = $_ if ($RecNo == 3);
    }
    close   VERF;
    return ($VerNumber, $VerBuild, $VerModel); 
}

sub GetLogTS
{
    my ($Time) = @_;
    my ($secs,$mins,$hrs,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($Time);
    my  $TStamp = sprintf "%d-%02d-%02d %02d:%02d:%02d",
                 ($year + 1900), ($mon + 1), $mday, $hrs, $mins, $secs;
    return $TStamp;
}

#
# Get yyyy/mm/dd hh:mm:ss from Unix Timestamp
#
sub GetTStamp
{
    my ($Time) = @_;
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($Time);
    my  $TStamp = sprintf "%4d/%02d/%02d %02d:%02d:%02d",
        $year + 1900, $mon + 1, $mday, $hour, $min, $sec;
    return ($TStamp);
}

sub GetYMDhmsTS
{
    my ($Time) = @_;
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($Time);
    my  $TStamp = sprintf "%4d%02d%02d_%02d%02d%02d",
        $year + 1900, $mon + 1, $mday, $hour, $min, $sec;
    return ($TStamp);

}

#
# Get Unix Timestamp from yyyy/mm/dd hh:mm:ss
#

sub GetUTS
{
    my  ($TS) = @_;
    my  ($YMD, $HMS) = split ' ', $TS;
    my  ($Y, $M, $D) = split '/', $YMD;
    my  ($H, $m, $S) = split ':', $HMS;
    my   $UTS = timelocal_nocheck ($S,$m,$H,$D,($M-1),($Y-1900));
    return $UTS;
}

sub LibLog
{
    my  ($LFile, $Msg, $Encoding) = @_;
    &CheckLog($LFile);
    if  (defined $Encoding)
    {
         die  "Could not open $LFile $!"
               unless open (LFILE, ">>:encoding($Encoding)", $LFile);
    }
    else
    {
         die  "Could not open $LFile $!"
               unless open (LFILE, ">>$LFile");
    }
    print LFILE "$Msg";
    #print      "$Msg";
    close LFILE;
}

sub GetFileMtime
{
    my ($FileN) = @_;
    return undef unless (-f $FileN);
    my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,$atime,$mtime,
                             $ctime,$blksize,$blocks) = stat($FileN); 
    return ($mtime); 
}

sub GetFileSize
{
    my ($FileN) = @_;
    return undef unless (-f $FileN);
    my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,$atime,$mtime,
                             $ctime,$blksize,$blocks) = stat($FileN);
    return ($size);
}

sub GetFileMtimeNSize
{
    my ($FileN) = @_;
    return (undef, undef) unless (-f $FileN);
    my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,$atime,$mtime,
                             $ctime,$blksize,$blocks) = stat($FileN);
    return ($mtime, $size);
}

sub CheckLog
{
    my ($FileN) = @_;
    my ($FDir, $FStub) = &GetFileDirNStub($FileN);
    mkdir $FDir unless (-d $FDir);
    my  $MSize  =  $Log{$FileN}->{"maxSize"};
    my  $MLogs  =  $Log{$FileN}->{"maxLogs"};
    return if  (!defined $MSize); 
    my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,$atime,$mtime,
                             $ctime,$blksize,$blocks) = stat($FileN);
    return if  ($MSize > $size); 
    return unless (-d $FDir);
    my  ($Log, @Logs);
    die "Could not open $FDir $!"
         unless opendir (DIR, $FDir); 
    (@Logs) = grep {/^$FStub/} readdir DIR;
    closedir DIR;
    for  $Log (sort {$b cmp $a} @Logs)
    {
         next unless ($Log =~ /^$FStub-(\d+)$/);
         my  $Index = $1;
         if ($Index == $MLogs)
         {
             unlink $Log;
             next;
         }
         rename "$FDir/$Log", "$FDir/$FStub-" . (sprintf "%02d", ($Index +1));
         next;
    }
    rename $FileN, "$FDir/$FStub-" . (sprintf "%02d", 0); 
}

sub GetFileDirNStub
{
    my ($FPName) = shift;
    my  $Pos    = rindex $FPName, '/';
    my ($PDir, $PName);
    $PName  = $FPName;
    $PName  = substr $FPName, ($Pos + 1) if  ($Pos >= 0);
    $PDir   = substr $FPName, 0 , $Pos   if  ($Pos >= 0);
    return   ($PDir, $PName);
}

#
# gets the IPv4NW address and BroadCast address from 
# IPv4 address and Network mask.
#
sub GetIPv4NetAddr
{
    my ($IPv4Addr, $IPv4NetMask) = @_;
    return (undef, undef) unless ($IPv4Addr    =~ /^\d+\.\d+\.\d+\.\d+$/ &&
                                  $IPv4NetMask =~ /^\d+\.\d+\.\d+\.\d+$/    );
    my  @IPv4Comps        = split(/\./,$IPv4Addr);
    my ($IPv4AddrB)       = unpack("N", pack("C4",@IPv4Comps));
    my  @IPv4NwMComps     = split(/\./,$IPv4NetMask);
    my ($IPv4NetMaskB )   = unpack("N", pack("C4",@IPv4NwMComps ));

    my  $IPv4NetAddrB     = ($IPv4AddrB & $IPv4NetMaskB);
    my  @IPv4NetAddrComps = unpack( "C4", pack("N",$IPv4NetAddrB));
    my  $IPv4NetAddr      = join(".",@IPv4NetAddrComps);
    my  $NBCastB          = ($IPv4AddrB & $IPv4NetMaskB) + ( ~ $IPv4NetMaskB);
    my  @NBCastComps      = unpack( "C4", pack("N",$NBCastB));
    my  $NBCast           = join (".", @NBCastComps);
    return ($IPv4NetAddr, $NBCast);
}

sub MntCache
{
    my ($CacheF) = shift;
    my  $MaxFiles = $Log{$CacheF}->{"maxLogs"};
    my ($PDir, $PName) = &GetFileDirNStub($CacheF);
    my ($File, $FileNo);
    die "Could not open $PDir $!"
         unless opendir (PDIR, $PDir);
    my (@Files) = grep {/^$PName\.\d+-\d+-\d+_\d+$/} readdir PDIR;
    closedir PDIR;
    for $File (sort {$b cmp $a} @Files)
    {
        $FileNo++;
        next if ($FileNo < $MaxFiles);
        unlink "$PDir/$File";
    }
}

sub CmpIPAddr
{
    my  ($IPa, $IPb) = @_;
    my  (@Comps) = split '\.', $IPa;
    my   $AddrA  = sprintf "%03d.%03d.%03d.%03d",
                   $Comps[0], $Comps[1], $Comps[2], $Comps[3];
    (@Comps)     = split '\.', $IPb;
    my   $AddrB  = sprintf "%03d.%03d.%03d.%03d",
                   $Comps[0], $Comps[1], $Comps[2], $Comps[3];
    return ($AddrA cmp $AddrB);
}

sub  StartRun
{
     my ($ProgDir, $ProgName, $Pid, $CmdName, $Option) = @_;
     my  $ProgRunF = "$ProgDir/$ProgName.run";
     if  (-f $ProgRunF)
     {
         die "Could not open $ProgRunF $!"
              unless open (PRUNF, $ProgRunF);
         my   $Rec = <PRUNF>;
         close PRUNF;
         $Rec =~ s/\s//g;
         my   $ProcD = "/proc/" .$Rec;
         if (($Rec !~ /^\s*$/) && (-d $ProcD))
         {
              my $CmdLineF = "$ProcD/cmdline";
              die  "Could not open $CmdLineF"
                    unless open (CMDF, $CmdLineF);
              my $Rec = <CMDF>;
              close CMDF;
              if  ((defined $CmdName && $Rec =~ /$CmdName/ ) &&
                  ((!defined $Option ) ||
                   ( defined $Option && $Rec =~ /$Option/  )))
              {
                   print STDERR "Cannot run $ProgName \n".
                                 "PID: $Rec is running !! \n";
                   return (0);
              }
         }
         unlink $ProgRunF;
     }
     die  "Could not open $ProgRunF $!"
           unless open (PRUNF, ">$ProgRunF");
     print PRUNF $Pid, "\n";
     close PRUNF;
     return 1;
}

sub  StopRun
{
     my ($ProgDir, $ProgName) = @_;
     my  $ProgRunF = "$ProgDir/$ProgName.run";
     unlink $ProgRunF if  (-f $ProgRunF);
}

1; 
