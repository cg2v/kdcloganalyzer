#!/usr/bin/perl
use strict;
our $lastu = undef;
our $count=0;
our $first=undef;
our $last=undef;
our $users=0;
our $records=0;
sub report {
   my ($limit)=@_;

   return if ($users < $limit);
   print STDERR "reporter:counter: Kerberos Analytics,activeusers,$users\n";
   $users=0;
   print STDERR "reporter:counter: Kerberos Analytics,transactions,$records\n";
   $records=0;
}

while (<>) {
   my ($u, $t)=split;
   if (defined($lastu) && $lastu ne $u) {
      printf "%s %s %s %s\n", $lastu, $count, $first, $last;
      undef $lastu;
      $users++;
   }
   if (!defined($lastu)){
      $lastu=$u;
      $count=0;
      $first=$t;
      $last=$t;
   }
   $first = $t if ($t lt $first);
   $last = $t if ($t gt $last);
   $count++;
   $records++;
   report(100);
}
report(0);
printf "%s %s %s %s\n", $lastu, $count, $first, $last;
exit(0);
