#!/bin/bash
for stage_num in `seq 3 5`;
do
    for replica_num in 2 4 10;
    do
        for key_num in 10 20 100 1000;
        do
            #echo $stage_num, $replica_num, $key_num
            python generate.py 1 $stage_num 3 $key_num 10 $replica_num
            #python wiscDecider.py > $stage_num+_+$key_num+_+$replica_num
            name=$stage_num+$replica_num+$key_num
            echo aa > result/$name
        done
    done
done


