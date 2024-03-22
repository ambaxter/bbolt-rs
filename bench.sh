#!/usr/bin/env bash


for options in {seq,rnd,seq-nest,rnd-nest}" "{seq,rnd}" "{1000,10000,100000}
do
  set -f; IFS=' '
  set -- $options
  for i in {1,5,10,25,50,100}
  do
    echo ./target/x86_64-apple-darwin/release/bench -w $1 -r $2 -c $3 -b $(($3/100*$i))
    ./target/x86_64-apple-darwin/release/bench -w $1 -r $2 -c $3 -b $(($3/100*$i))
  done
done