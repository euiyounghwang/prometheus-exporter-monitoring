#!/bin/bash
set -e

JAVA_HOME='/home/biadmin/jdk1.8.0_151'
PATH=$PATH:$JAVA_HOME
export JAVA_HOME

# Activate virtualenv && run serivce

SCRIPTDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd $SCRIPTDIR

VENV=".venv"

# Python 3.11.7 with Window
if [ -d "$VENV/bin" ]; then
    source $VENV/bin/activate
else
    source $VENV/Scripts/activate
fi


# -- Export Variable
export GRAFANA_DASHBOARD_URL="http://localhost:3000/d/adm08055cf3lsa/es-team-dashboard?orgId=1'&'from=now-5m'&'to=now'&'refresh=5s"
export SMTP_HOST="localhost"
export SMTP_PORT=25
export ZOOKEEPER_URLS="localhost:2181,localhost:2181,localhost:2181"
export BROKER_LIST="localhost:9092,localhost:9092,localhost:9092"
export GET_KAFKA_ISR_LIST="$SCRIPTDIR/kafka_2.11-0.11.0.0/bin/kafka-topics.sh --describe --zookeeper $ZOOKEEPER_URLS --topic ELASTIC_PIPELINE_QUEUE"
export KAFKA_JOB_INTERFACE_API="localhost:8008"
export ES_NODES_DISK_AVAILABLE_THRESHOLD="10"
export ES_HOST_URL_PREFIX="https"
# -- 

# -- standalone type
# local
# server : --first node of kafka_url is a master node to get the number of jobs using http://localhost:8080/json

# -- direct access to db
#python ./standalone-es-service-export.py --interface db --url jdbc:oracle:thin:id/passwd@address:port/test_db --db_run false --kafka_url localhost:9092,localhost:9092,localhost:9092 --kafka_connect_url localhost:8083,localhost:8083,localhost:8083 --zookeeper_url  localhost:2181,localhost:2181,localhost:2181 --es_url localhost:9200,localhost:9201,localhost:9201,localhost:9200 --kibana_url localhost:5601 --sql "SELECT processname from test"

# -- collect records through DB interface Restapi
python ./standalone-es-service-export.py --interface http --db_http_host localhost:8002 --url jdbc:oracle:thin:id/passwd@address:port/test_db --db_run false --kafka_url localhost:9092,localhost:9092,localhost:9092 --kafka_connect_url localhost:8083,localhost:8083,localhost:8083 --zookeeper_url  localhost:2181,localhost:2181,localhost:2181 --es_url localhost:9200,localhost:9201,localhost:9201,localhost:9200 --kibana_url localhost:5601 --sql "SELECT processname from test"
