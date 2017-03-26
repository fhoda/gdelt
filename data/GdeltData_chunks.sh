#!/bin/bash

for i in 1 2 3 4 5 6 7 8 9; do
	for j in {1..9}; do
		echo "$10$i"0"$j" 
		aws s3 cp s3://gdelt-open-data/events/"$10$i"0"$j".export.csv .
	done
done

hadoop fs -put $1* /gdelt/input
rm -f $1*
hadoop jar ../app/Gdelt.jar Gdelt -Dviolence=/gdelt/violence.txt -Dpeace=/gdelt/peace.txt /gdelt/input/ /gdelt/output/"$1"_1
hadoop fs -rm -skipTrash /gdelt/input/*

for i in 1 2 3 4 5 6 7 8 9; do
        for j in {10..20}; do
                echo $10$i$j
                aws s3 cp s3://gdelt-open-data/events/$10$i$j.export.csv .
        done
done

hadoop fs -put $1* /gdelt/input
rm -f $1*
hadoop jar ../app/Gdelt.jar Gdelt -Dviolence=/gdelt/violence.txt -Dpeace=/gdelt/peace.txt /gdelt/input/ /gdelt/output/"$1"_2
hadoop fs -rm -skipTrash /gdelt/input/*


for i in 1 2 3 4 5 6 7 8 9; do
        for j in {21..31}; do
                echo $10$i$j
                aws s3 cp s3://gdelt-open-data/events/$10$i$j.export.csv .
        done
done

hadoop fs -put $1* /gdelt/input
rm -f $1*
hadoop jar ../app/Gdelt.jar Gdelt -Dviolence=/gdelt/violence.txt -Dpeace=/gdelt/peace.txt /gdelt/input/ /gdelt/output/"$1"_3
hadoop fs -rm -skipTrash /gdelt/input/*

for i in 10 11 12; do
        for j in {1..9}; do
                echo "$1$i"0"$j"
                aws s3 cp s3://gdelt-open-data/events/"$1$i"0"$j".export.csv .
        done
done

hadoop fs -put $1* /gdelt/input
rm -f $1*
hadoop jar ../app/Gdelt.jar Gdelt -Dviolence=/gdelt/violence.txt -Dpeace=/gdelt/peace.txt /gdelt/input/ /gdelt/output/"$1"_4
hadoop fs -rm -skipTrash /gdelt/input/*

for i in 10 11 12; do
	for j in {10..20}; do
		echo $1$i$j
		aws s3 cp s3://gdelt-open-data/events/$1$i$j.export.csv .
	done
done

hadoop fs -put $1* /gdelt/input
rm -f $1*
hadoop jar ../app/Gdelt.jar Gdelt -Dviolence=/gdelt/violence.txt -Dpeace=/gdelt/peace.txt /gdelt/input/ /gdelt/output/"$1"_5
hadoop fs -rm -skipTrash /gdelt/input/*

for i in 10 11 12; do
        for j in {20..31}; do
                echo $1$i$j
                aws s3 cp s3://gdelt-open-data/events/$1$i$j.export.csv .
        done
done

hadoop fs -put $1* /gdelt/input
rm -f $1*
hadoop jar ../app/Gdelt.jar Gdelt -Dviolence=/gdelt/violence.txt -Dpeace=/gdelt/peace.txt /gdelt/input/ /gdelt/output/"$1"_6
hadoop fs -rm -skipTrash /gdelt/input/*


hadoop jar ../app/GdeltChunks.jar GdeltChunks /gdelt/output/$1_* /gdelt/output/$1

hadoop fs -get /gdelt/output/$1/part*
mv part* ../output/$1.txt
