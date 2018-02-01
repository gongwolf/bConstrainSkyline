#!/bin/bash

for size in {1000,2000,4000,6000,8000}; 
do
#    for degree in {1,2,3,4};
#    do
        degree=5
        echo ${size}_$degree
        command=$(java -jar ~/shared_git/bConstrainSkyline/classes/artifacts/qixugong_jar/qixugong.jar ${size} ${degree} 250)
        echo "$command" >> output/${size}_${degree}_bs_output.txt
#    done
done
