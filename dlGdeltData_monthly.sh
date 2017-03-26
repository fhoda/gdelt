#!/bin/bash

for i in 1 2 3 4 5 6 7 8 9; do
	echo $10$i
	aws s3 cp s3://gdelt-open-data/events/$10$i.csv .
done

for i in 10 11 12; do
	echo $1$i
	aws s3 cp s3://gdelt-open-data/events/$1$i.csv .
done

hadoop fs -put 2* /gdelt/input
hadoop fs -ls /gdelt/input
