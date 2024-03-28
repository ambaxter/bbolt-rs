#!/usr/bin/env bash

CMD=./target/release/bench

for options in {seq,rnd,seq-nest,rnd-nest}" "{seq,rnd}" "{100000,1000000}
do
  set -f; IFS=' '
  set -- $options
  for i in {5000,10000,25000,50000,100000,250000,500000}
  do
    BATCH=$i
    if [[ "$BATCH" -gt "$3" ]] ; then
      continue
    fi
    if [[ "$1" -eq "rnd" ]] && [[ "$BATCH" -gt "250000" ]] ; then
      continue
    fi
    for j in {1,2,3,4,5}; do
      RUN=$($CMD -w $1 -r $2 -c $3 -b $BATCH 2>&1 | awk '{sub(/\(/, "", $5);print $5}' | tr '\n' ',' | sed 's/,,//')
      echo $1,$2,$3,$BATCH,$RUN | awk -F',' '{print "unsafe," $1 "," $2 "," $3 "," $4 "," $5 "," $6}'
    done
  done
done