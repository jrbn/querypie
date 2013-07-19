#!/bin/bash

cd `dirname $0`
for JAR in `find ../lib/ -name *.jar`;
do
CLASSPATH=$CLASSPATH:$JAR
done
for JAR in ../javagat/*.jar
do
CLASSPATH=$CLASSPATH:$JAR
done

for JAR in `find ../../arch/lib/ -name *.jar`;
do
CLASSPATH=$CLASSPATH:$JAR
done

CLASSPATH=$CLASSPATH:../conf
CLASSPATH=$CLASSPATH:../querypie.jar
CLASSPATH=${CLASSPATH:1}

JAVA_VM="-Xmx15G -Dgat.adaptor.path=`pwd`/../javagat/adaptors -Djava.library.path=../lib/lzo/native/Linux-amd64-64 -Djava.io.tmpdir=/tmp/querypie_tmp"
JAVA_OPTS="$JAVA_VM -cp $CLASSPATH"

export JAVA_OPTS
export CLASSPATH
