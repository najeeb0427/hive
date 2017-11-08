#!/bin/bash

. ./config.properties

info_log()
{
  echo "In info log"
  echo "INFO    `date '+%Y-%m-%d %H:%M:%S'`    $1" >> ${LOG_FILE}
}

error_log()
{
  echo "ERROR    `date '+%Y-%m-%d %H:%M:%S'`    $1" >> ${LOG_FILE}
}

d=`echo $(date +%Y-%m)`

mkdir -p ${LOCAL_BASE_PATH}/${d}

cd ${LOCAL_BASE_PATH}/${d}
if [ -f ${LOCAL_BASE_PATH}/${d}/_DONE ]
then
        info_log "Done File Found. Skippig wget and unzip"
else
        rm -rf ${LOCAL_BASE_PATH}/${d}/*
        wget $URL
        if [ $? -eq 0 ]
        then
                info_log "File Download Success."
        else
                error_log "File download Failed. Exiting"
                exit -1
        fi
        unzip ${LOCAL_BASE_PATH}/${d}/${ZIP_FILE_NAMEi}
        if [ $? -eq 0 ]
        then
                info_log "File Unzipping Success."
                touch ${LOCAL_BASE_PATH}/${d}/_DONE
        else
                error_log "File Unzip Failed. Exiting"
                exit -1
        fi
fi

hadoop fs -mkdir ${HDFS_BASE_PATH}/${d}
hadoop fs -put ${LOCAL_BASE_PATH}/${d}/*.csv ${HDFS_BASE_PATH}/${d}