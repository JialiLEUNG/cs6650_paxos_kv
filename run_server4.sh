#!/usr/bin/env bash

cur_path=`pwd`
echo ${cur_path}
java -jar \
${cur_path}/classes/artifacts/Server4_jar/KvPaxos.jar ${cur_path}/serverConfiguration/server4conf.json