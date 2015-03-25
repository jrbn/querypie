#!/bin/bash

cd `dirname $0`

QUERYPIE=`pwd`/..

CLASSPATH=$CLASSPATH:$QUERYPIE/conf
CLASSPATH=$CLASSPATH:$QUERYPIE/querypie-full.jar
CLASSPATH=${CLASSPATH:1}

JAVA_VM=" -Dlogback.configurationFile=$QUERYPIE/conf/logback.xml -Dgat.adaptor.path=$QUERYPIE/javagat/adaptors -Djava.io.tmpdir=/tmp/querypie_tmp"
JAVA_OPTS="$JAVA_VM -Xmx3G -cp $CLASSPATH"

export JAVA_OPTS
export CLASSPATH
