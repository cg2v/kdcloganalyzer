#!/usr/bin/perl
use strict;
our $svc=undef;
our $svct=undef;
our $et=undef;
our $records=0;
our $errorentries=0;
our $referrals=0;
our $badreq=0;
our $badauth=0;
our $badpass=0;
sub report {
    my ($limit) = @_;
    return if ($records < $limit);
    print STDERR "reporter:counter:Kerberos Log,accepted,$records\n";
    $records=0;
    if ($referrals >= $limit) {
       print STDERR "reporter:counter:Kerberos Log,referrals,$referrals\n";
       $referrals=0;
    }
    if ($errorentries >= $limit) {
       print STDERR "reporter:counter:Kerberos Log,errorsfound,$errorentries\n";
       $errorentries=0;
    }
    if ($badreq >= $limit) {
       print STDERR "reporter:counter:Kerberos Log,invalidrequest,$badreq\n";
       $badreq=0;
    }
    if ($badauth >= $limit) {
       print STDERR "reporter:counter:Kerberos Log,invalidauthentication,$badauth\n";
       $badauth=0;
    }
    if ($badpass >= $limit) {
       print STDERR "reporter:counter:Kerberos Log,badpassword,$badpass\n";
       $badpass=0;
    }
}
while (<>) {
   if (/(\S+)\s+\S+\s+Pre-authentication succeeded --\s+(\S+)\@(\S+) using (\S+)/) {
       print "u:" . $2 . "/$4\t" . $1 . "\n";
       $records++;
}   elsif (/(\S+)\s+\S+\s+Pre-authentication succeeded --\s+(\S+)\@ANDREW.CMU.EDU/) {
       print "u:" . $2 . "/UNK\t" . $1 . "\n";
       $records++;
   }
   if (/(\S+)\s+TGS-REQ\s+.*\s+for\s+(\S+)\@ANDREW.CMU.EDU/) {
       $svc=$2;
       $svct=$1;
       $et="UNK"
   }
   if (/\S+\s+sending/) {
      if (defined($svc)) {
          print "s:" . $svc . "/" . $et . "\t" . $svct . "\n";
          $records++;
      }
      $svc = undef;
   }
   if (/[Rr]eturning a referral to realm/) {
      $referrals++;
      $svc = undef;
   }
   if (/UNKNOWN|Server not found in database|\s+expired\s/) {
     
      $errorentries++;
      $badreq++;
      $svc = undef;
    }
    if (/krb_rd_req:|No key matches pa-data|Too large time skew|Failed building TGS-REP/) {
      $svc = undef;
      $errorentries++;
      $badauth++;
    }
    if (/Failed to decrypt PA-DATA --/) {
      $svc = undef;
      $errorentries++;
      $badpass++;
    }
    if (/Client supported enctypes: (.*) using (\S+)/) {
      my @etl=split /,\s*/, $1;
      $et = join("/", $etl[0], $etl[-1], $2);
    }
    report(100);
}
report(0);
exit(0);
