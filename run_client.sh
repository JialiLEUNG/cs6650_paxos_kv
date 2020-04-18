#!/usr/bin/env bash

cur_path=`pwd`
echo ${cur_path}
java -jar \
${cur_path}/classes/artifacts/Client_jar/KvPaxos.jar 30001 ${cur_path}/src/main/java/com/github/jiali/paxos/clientRequests/ClientRequest.txt