#!/usr/bin/env bash

CMD=./target/release/bench

for options in {seq,rnd,seq-nest,rnd-nest}" "{seq,rnd}" "{1000,10000,100000,1000000}
do
  set -f; IFS=' '
  set -- $options
  for i in {1,5,10,25,50,100}
  do
    BATCH=$(($3*$i/100))
    if [[ "$BATCH" -gt 100000 ]] ; then
      continue
    fi
    echo $CMD -w $1 -r $2 -c $3 -b $BATCH
    $CMD -w $1 -r $2 -c $3 -b $BATCH
  done
done