#!/usr/bin/perl

use warnings;
use strict;
use Redis;
use Data::Dumper;
use lib '.';
use RdbParser;

        my $callbacks = {
#            "start_rdb"         => \&start_rdb,
            "start_database"    => \&start_database,
            "key"               => \&key,
            "set"               => \&set,
            "start_hash"        => \&start_hash,
            "hset"              => \&hset,
            "end_hash"          => \&end_hash,
            "start_set"         => \&start_set,
            "sadd"              => \&sadd,
            "end_set"           => \&end_set,
            "start_list"        => \&start_list,
            "rpush"             => \&rpush,
            "end_list"          => \&end_list,
            "start_sorted_set"  => \&start_sorted_set,
            "zadd"              => \&zadd,
            "end_sorted_set"    => \&end_sorted_set,
            "end_database"      => \&end_database,
#            "end_rdb"           => \&end_rdb,
        };

my $redis = Redis->new;
$redis->wait_all_responses;

my $parser = new Redis::RdbParser($callbacks);
$parser->parse('dump.rdb');

my $processed = 0;
sub key {
  $processed++;
  if ( $processed > 1000 ) { $redis -> exec; $redis -> multi; $processed = 0; }
}

sub start_database {
  $redis -> multi;
}

sub end_database {
  $redis -> exec;
}

sub set {
  my ($key, $value, $expiry) = @_;
  $redis -> set ($key, $value);
}

sub start_hash {
  my ($key, $length, $expiry, $info) = @_;
#  $redis -> multi;
}

sub end_hash {
#  $redis -> exec;
}

sub hset {
  my ($key, $field, $value) = @_;
  $redis -> hset( $key, $field => $value );
}

sub start_set {
#  $redis -> multi;
}

sub sadd {
  my ($key, $member) = @_;
  $redis -> sadd( $key, $member );
}

sub end_set {
#  $redis -> exec;
}

sub start_list {
#  $redis -> multi;
}

sub rpush {
  my ($key, $value) = @_;
  $redis -> rpush( $key, $value );
}

sub end_list {
#  $redis -> exec;
}

sub start_sorted_set {
#  $redis -> multi;
}

sub zadd {
  my ($key, $score, $member) = @_;
  $redis -> zadd( $key, $score, $member );
}

sub end_sorted_set {
#  $redis -> exec;
}
