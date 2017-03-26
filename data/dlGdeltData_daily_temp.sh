#!/bin/bash

for i in 1 2 3 4 5 6 7 8 9; do
	for j in {1..9}; do
		echo "$10$i"0"$j"
		aws s3 cp s3://gdelt-open-data/events/"$10$i"0"$j".export.csv .
	done
done

for i in 10 11 12; do
        for j in {1..9}; do
                echo "$10$i"0"$j"
                aws s3 cp s3://gdelt-open-data/events/"$10$i"0"$j".export.csv .
        done
done

hadoop fs -put $1* /gdelt/input
rm -f $1*
