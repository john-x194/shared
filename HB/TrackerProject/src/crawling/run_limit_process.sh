#! /bin/sh

command="python3.6 $1"
echo $command
pid=$!
procs=`echo {0..4..1}`
echo $procs
# taskset -c 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31 $command  # start a command with the given affinity

  