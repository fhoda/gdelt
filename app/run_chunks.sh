echo $i
hadoop jar Gdelt.jar Gdelt -Dviolence=/gdelt/violence.txt -Dpeace=/gdelt /peace.txt /gdelt/input/ /gdelt/output/$i
hadoop fs -cat /gdelt/output/$i/part*
