#!/bin/bash

if [ $# -lt 2 ];
then
    echo "Usage: querypie.sh <address ibis> <absolute input dir> [--rules file_rules] [ --cleanCache ] [ --cacheDir <dir cache> ] [ --cacheURLs <file cache URLs> ]"
exit;
fi

cd `dirname $0`
. ./init-env.sh

#Go through all parameters
CACHE=
CLEAN=
CACHE_URLS=
CACHING_ITER=
RULES=../conf/owl2_rules

IBIS_SERVER=${1}
INPUT_DIR=${2}

while (( "$#" )); do

if [[ ${1} == "--reserve" ]]; then
shift
RESERVATION="-reserve ${1}"
elif [[ ${1} == "--cleanCache" ]]; then
CLEAN="--clean-cache"
elif [[ ${1} == "--cachingIterator" ]]; then
CACHING_ITER="--caching-iterator"
elif [[ ${1} == "--javagat" ]]; then
JAVAGAT="--javagat"
elif [[ ${1} == "--cacheDir" ]]; then
shift
CACHE="--cache-location ${1}"
elif [[ ${1} == "--rules" ]]; then
shift
RULES="${1}"
elif [[ ${1} == "--cacheURLs" ]]; then
shift
CACHE_URLS="--cacheURLs ${1}"
fi
shift
done

#Get random pool name
POOL_NAME="reasoner-$RANDOM"
IBIS_OPTS="-Dibis.pool.name=$POOL_NAME -Dibis.server.address=$IBIS_SERVER -Dibis.pool.size=1"

echo "Pool name = $POOL_NAME"

#Launch the program
java $JAVA_OPTS $IBIS_OPTS  -Drules.list=$RULES nl.vu.cs.querypie.QueryPIE $INPUT_DIR --n-proc-threads 1 $CACHE $CLEAN $CACHE_URLS $JAVAGAT $CACHING_ITER > out-querypie 2>&1
