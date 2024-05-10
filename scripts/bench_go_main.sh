#!/usr/bin/env bash

CMD=./bin/bbolt

for options in {seq,rnd,seq-nest,rnd-nest}" "{seq,rnd}" "{100000,1000000}
do
  set -f; IFS=' '
  set -- $options
  for i in {5000,10000,25000,50000,100000,250000,500000}
  do
    BATCH=$i
    if [[ "$BATCH" -gt $3 ]] ; then
      continue
    fi
    if [[ "$1" == "rnd" ]] && [[ "$BATCH" -gt "100000" ]] ; then
      continue
    fi
    if [[ "$1" == "rnd-nest" ]] && [[ "$BATCH" -gt "50000" ]] ; then
      continue
    fi
    for j in {1,2,3,4,5}; do
      RUN=$($CMD bench -write-mode $1 -read-mode $2 -count $3 -batch-size $BATCH -profile-mode n 2>&1 | grep "#" | awk '{sub(/\(/, "", $6);print $6}' | tr '\n' ',' | sed 's/,,//')
      echo $1,$2,$3,$BATCH,$RUN | awk -F',' '{print "go_main," $1 "," $2 "," $3 "," $4 "," $5 "," $6}'
    done
  done
done