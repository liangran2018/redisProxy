#!/bin/sh
echo "monitor"

while true
do
    stillRunning=`ps -ef | grep "thrift" | grep -v 'grep' | awk '{print $2}'`
#result=$(echo stillRunning | grep "thrift")
#echo $stillRunning
#localHost="`hostname --fqdn`"
#local_ip=`host $localHost 2>/dev/null`
    TCPListening=`ps -ef | netstat -an | grep 13926 | awk '{print $2}'`
#echo $TCPListening
    if [ "$stillRunning" != "" -o "$TCPListening" != "" ]
    then
        sleep 1
    else
        echo "thrift service was exited!"
        ./thrift
        sleep 1
    fi

done