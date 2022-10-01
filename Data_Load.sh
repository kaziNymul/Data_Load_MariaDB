#!/bin/bash

cd /home/cdr_edr/cdr_history/

#remove files older than 10 days from "done" folder
#find ./done/i*.gz -type f -mtime +10 -exec rm -f {} \; 

#compress T files in done folder
#find ./done/*.CDR -type f -exec gzip {} \; 2>/dev/null

#compress log files oldrer than 5 days
#find ./log/log*.csv -type f -mtime +5 -exec gzip {} \; 2>/dev/null

date_time1=`date +"%Y%m%d"`

date_time2=`date +"%Y-%m-%d %H:%M:%S"`

#parameters file contains the folders used by script
#first line is the directory where EMM put files
#second line is the "work" directory where the script keep files temporarily
#third line is the "done" directory where files are put after processing
#fourth line is the error directory
mapfile -t <parameters
cdr_in=`echo "${MAPFILE[0]}"`
cdr_work=`echo "${MAPFILE[1]}"`
cdr_done=`echo "${MAPFILE[2]}"`
cdr_error=`echo "${MAPFILE[3]}"`
cdr_log=`echo "${MAPFILE[4]}"`

#check if script is already running to avoid duplicate processing
for pid in $(pidof -x new_load_files.sh); do
    if [ $pid != $$ ]; then
        echo "[$(date)] : new_load_files.sh : Process is already running with PID $pid" >> ${cdr_work}log_file.csv
        exit 1
    fi
done

#check if any files are in work directory - this indicates previous run failed
if ls ${cdr_work}* > /dev/null 2>&1
then
    files=${cdr_work}*
    echo $files > ${cdr_work}log_file.csv
    echo "File(s) above were found in work directory! If error persists, please check and clean work directory" >> ${cdr_work}log_file.csv
    echo "Exit without loading." >> ${cdr_work}log_file.csv
    echo "Stopped on error: "$date_time2 >> ${cdr_work}log_file.csv
    echo "" >> ${cdr_work}log_file.csv
    #mutt -s "Error in HDS CDR loading. File in work folder." jonas.wegelius@ericsson.com < ./work/log_file.csv
    cat ${cdr_work}log_file.csv >> ${cdr_log}log_file_${date_time1}.csv
    rm ${cdr_work}log_file.csv
    exit 0
fi

echo "STARTING "$date_time2 > ${cdr_work}log_file.csv

check_files=`find ${cdr_in} -maxdepth 1 -type f -name '*.CDR'|wc -l`

if [[ $check_files > 0 ]]
#check that cdr files exist - if there are no files, exit without action
then
	for newfiles in $(find $cdr_in -maxdepth 1 -name '*.CDR' | head -4000)
	do
		cdr_file=`ls $newfiles`
		cat ${newfiles} >> ${cdr_work}load_file.csv
		#cp ${cdr_work}load_file.csv ${cdr_log}load_file.csv
                dir_file=`echo ${newfiles}`
                count_lines=`wc -l ${newfiles} | sed 's/ .*//'`
                date_time=`date +"%Y-%m-%d %H:%M:%S"`

		#the header files will be used to log file metrics in tracking table
                echo $cdr_file','$date_time','$count_lines >> ${cdr_work}header_file.csv

                date_time=`date +"%Y%m%d%H%M%S"`
		mv ${newfiles} $cdr_work
	done

        echo "Files, timestamp and number of lines to be handled: " >> ${cdr_work}log_file.csv
	cat ${cdr_work}header_file.csv >> ${cdr_work}log_file.csv

	#log in to HDS database using dbss_cdr_history schema
	#first truncate the working tables
	#then load the two csv files into working tables
	#then call procedure to process new records
cd ${cdr_work}
#mysql -u migration dbss_cdr_history -h10.74.10.177 >> ${cdr_work}log_file.csv << EOF
mysql -u migration dbss_cdr_history -h10.74.10.177 >> ${cdr_work}log_file.csv << EOF
truncate table log_cdr_files0;
truncate table load_cdr_file;
truncate table load_cdr_file2;
load data local infile 'header_file.csv' into table log_cdr_files0 FIELDS TERMINATED BY ',' (file_name, log_time, record_count);
load data local infile 'load_file.csv' into table load_cdr_file FIELDS TERMINATED BY ',' ;
select count(1) into @count_cdr from load_cdr_file;
select count(1) into @count_file from log_cdr_files0;
select CONCAT('Number of files to be loaded: ', @count_file);
select CONCAT('Number of records to be loaded: ', @count_cdr);
call LoadCdrFile(@result);
select CONCAT('Result of stored procedure: ', @result);
commit;
EOF

        rm ${cdr_work}load_file.csv
        rm ${cdr_work}header_file.csv
        sed -i '/CONCAT/d' ${cdr_work}log_file.csv
else
        echo "No files to process at "$date_time2 >> ${cdr_work}log_file.csv
fi

date_time=`date +"%Y-%m-%d %H:%M:%S"`
echo "DONE "$date_time >> ${cdr_work}log_file.csv
echo "-----" >> ${cdr_work}log_file.csv

cd $cdr_work

touch *.CDR
mv *.CDR $cdr_done

#put all text into daily log file
cat ${cdr_work}log_file.csv >> ${cdr_log}log_file_${date_time1}.csv
rm ${cdr_work}log_file.csv
sleep 10
