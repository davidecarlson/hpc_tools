#!/usr/bin/env perl
# Emacs: -*- mode: cperl; -*-

# Author: Tony Curtis

use strict;
use warnings;

my $daxctl = 'daxctl';

open my $dh, "$daxctl list |" or die "Can't run $daxctl: $!\n";

my %hbm_numa_nodes;

while (<$dh>) {
  /"target_node":(\d+)/ or next;
  $hbm_numa_nodes{$1} = $1;
}

close $dh;

my $numactl = 'numactl';

open my $nh, "$numactl -H |" or die "Can't run $numactl: $!\n";

my $total_mb = 0;
my $free_mb = 0;

while (<$nh>) {

  /^node (\d+) (\w+):\s+(\d+)/ or next;

  next unless defined $hbm_numa_nodes{$1};

  if ($2 eq 'size') {
    $total_mb += $3;
    next;
  }

  if ($2 eq 'free') {
    $free_mb += $3;
    next;
  }

}

close $nh;

my $used_mb = $total_mb - $free_mb;

my $total_gb = $total_mb / 1024;
my $used_gb  = $used_mb  / 1024;
my $free_gb  = $free_mb  / 1024;

printf "Total HBM = %6d GB    %6d MB\n", $total_gb, $total_mb;
printf "Used  HBM = %6d GB    %6d MB\n", $used_gb,  $used_mb;
printf "Free  HBM = %6d GB    %6d MB\n", $free_gb,  $free_mb;
