set -e
YEAR=2000
SCRIPT_HOME=`pwd`
while [  $YEAR -lt 2018 ]; do
	echo The year is $YEAR
	wget -r ftp://ftp.ncdc.noaa.gov/pub/data/noaa/${YEAR}/
	DIR=ftp.ncdc.noaa.gov/pub/data/noaa/
	cd ${DIR}
	ls ${YEAR}
	aws s3 cp ${YEAR} s3://paulhtremblay/noa1/${YEAR} --recursive
	rm -Rf ${YEAR}
	cd ${SCRIPT_HOME}
	let YEAR=YEAR+1 
done
