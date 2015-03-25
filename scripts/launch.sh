#!/bin/bash

cd `dirname $0`
. ./init-env.sh

java $JAVA_OPTS $IBIS_OPTS ${*}

