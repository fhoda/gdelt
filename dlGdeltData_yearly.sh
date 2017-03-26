
#!/bin/bash

for i in "$@"; do
	echo $i
	aws s3 cp s3://gdelt-open-data/events/$i.csv .
done
