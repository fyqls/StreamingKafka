#!/bin/bash
JOBSERVERURL=172.16.1.130:8090
ZKSERVERS=172.16.1.131:2181,172.16.1.130:2181

TOPIC=test
curl --data-binary @StreamingTest.jar $JOBSERVERURL/jars/StreamingTest
curl -d "zk=\"$ZKSERVERS\",topic=\"$TOPIC\"" $JOBSERVERURL'/jobs?appName=StreamingTest&classPath=EmptyStreaming&context=jobapp&sync=false'

