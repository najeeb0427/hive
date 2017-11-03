#!/bin/bash

d=`echo $(date +%Y-%m)`
url="http://fracfocusdata.org/digitaldownload/fracfocuscsv.zip"
hdfs_path="/user/data/"

mkdir -p /tmp/FracFocus_Data/${d}

cd /tmp/FracFocus_Data/${d}
if [ -f /tmp/FracFocus_Data/${d}/_DONE ]
then
	        echo "Done File Found. Skippig wget"
		        unzip /tmp/FracFocus_Data/${d}/fracfocuscsv.zip
		else
			        rm -rf /tmp/FracFocus_Data/${d}/*
				        wget $url
					        if [ $? -eq 0 ]
							        then
									                echo "File Download Success."
											                touch /tmp/FracFocus_Data/${d}/_DONE
													        else
															                echo "File download Failed. Exiting"
																	        fi
																	fi

																	hadoop fs -mkdir ${hdfs_path}/${d}
																	hadoop fs -put /tmp/FracFocus_Data/${d}/*.csv ${hdfs_path}/${d}
