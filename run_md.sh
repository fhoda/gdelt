#!/bin/bash

for i in "$@"; do
	echo $i
	hadoop jar Gdelt.jar Gdelt -Dviolence=/gdelt/violence.txt -Dpeace=/gdelt/peace.txt /gdelt/input/ /gdelt/output/$i
	hadoop fs -cat /gdelt/output/$i/part*
	hadoop fs -get /gdelt/output/$i/part*
	mv part* ../output/$i.txt
	rm -rf part*
done

ls ../output

