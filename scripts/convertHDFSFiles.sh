#!/bin/bash

if [ $# -lt 2 ];
then
echo "Usage: convertHDFSFiles.sh <absolute input dir> <absolute ouput dir>"
exit;
fi

cd `dirname $0`
. ./init-env.sh

OUTPUT_DIR=${2}
INDEX_DIR="${OUTPUT_DIR}/index"
CLOSURE_DIR="${OUTPUT_DIR}/closure"

echo "Prepare directory $OUTPUT_DIR ..."
if [ -d $OUTPUT_DIR ];
then
echo "-> Remove old dir ..."
rm -rf $OUTPUT_DIR
fi

mkdir $OUTPUT_DIR
mkdir $INDEX_DIR
mkdir $CLOSURE_DIR

INDEXES="spo sop pos ops pso osp"
for INDEX in $INDEXES; do
    echo "process index $INDEX ..."
    mkdir "${INDEX_DIR}/${INDEX}"
    find ${1}/_index/${INDEX}/index -type f -print | xargs ./docopy ${1}/_index/${INDEX}/index ${INDEX_DIR}/${INDEX}
done

echo ""
echo "Convert schema triples ... "

SCHEMA_SUBSETS[0]="rdfs-subproperties"
SCHEMA_SUBSETS[1]="rdfs-subclasses"
SCHEMA_SUBSETS[2]="rdfs-domain"
SCHEMA_SUBSETS[3]="rdfs-range"
SCHEMA_SUBSETS[4]="owl-inverse"
SCHEMA_SUBSETS[5]="owl-transitive-properties"
SCHEMA_SUBSETS[6]="owl-symmetric-properties"
SCHEMA_SUBSETS[7]="owl-on-properties"
SCHEMA_SUBSETS[8]="owl-some-values-from"
SCHEMA_SUBSETS[9]="owl-all-values-from"
SCHEMA_SUBSETS[10]="owl-has-value"
SCHEMA_SUBSETS[11]="owl-functional-properties"
SCHEMA_SUBSETS[12]="owl-inverse-functional-properties"
SCHEMA_SUBSETS[13]="owl-equivalence-classes"
SCHEMA_SUBSETS[14]="owl-equivalence-properties"
SCHEMA_SUBSETS[15]="owl2-props-axioms"
SCHEMA_SUBSETS[16]="owl2-has-key"
SCHEMA_SUBSETS[17]="owl2-intersection-of"
SCHEMA_SUBSETS[18]="owl2-union-of"
SCHEMA_SUBSETS[19]="owl2-one-of"
SCHEMA_SUBSETS[20]="owl2-max-card"
SCHEMA_SUBSETS[21]="owl2-max-q-card"
SCHEMA_SUBSETS[22]="owl2-on-class"
SCHEMA_SUBSETS[23]="owl-type-class"
SCHEMA_SUBSETS[24]="owl-datatype-prop"
SCHEMA_SUBSETS[25]="owl-objtype-prop"
SCHEMA_SUBSETS[26]="owl-same-as"

FILTER_SCHEMA[0]="FILTER_ONLY_SUBPROP_SCHEMA"
FILTER_SCHEMA[1]="FILTER_ONLY_SUBCLASS_SCHEMA"
FILTER_SCHEMA[2]="FILTER_ONLY_DOMAIN_SCHEMA"
FILTER_SCHEMA[3]="FILTER_ONLY_RANGE_SCHEMA"
FILTER_SCHEMA[4]="FILTER_ONLY_OWL_INVERSE_OF"
FILTER_SCHEMA[5]="FILTER_ONLY_OWL_TRANSITIVE_SCHEMA"
FILTER_SCHEMA[6]="FILTER_ONLY_OWL_SYMMETRIC_SCHEMA"
FILTER_SCHEMA[7]="FILTER_ONLY_OWL_ON_PROPERTY"
FILTER_SCHEMA[8]="FILTER_ONLY_OWL_SOME_VALUES"
FILTER_SCHEMA[9]="FILTER_ONLY_OWL_ALL_VALUES"
FILTER_SCHEMA[10]="FILTER_ONLY_OWL_HAS_VALUE"
FILTER_SCHEMA[11]="FILTER_ONLY_OWL_FUNCTIONAL_SCHEMA"
FILTER_SCHEMA[12]="FILTER_ONLY_OWL_INVERSE_FUNCTIONAL_SCHEMA"
FILTER_SCHEMA[13]="FILTER_ONLY_EQ_CLASSES"
FILTER_SCHEMA[14]="FILTER_ONLY_EQ_PROPERTIES"
FILTER_SCHEMA[15]="FILTER_ONLY_OWL_CHAIN_PROPERTIES"
FILTER_SCHEMA[16]="FILTER_ONLY_OWL_HAS_KEY"
FILTER_SCHEMA[17]="FILTER_ONLY_OWL_INTERSECTION_OF"
FILTER_SCHEMA[18]="FILTER_ONLY_OWL_UNION_OF"
FILTER_SCHEMA[19]="FILTER_ONLY_OWL_ONE_OF"
FILTER_SCHEMA[20]="FILTER_ONLY_OWL_MAX_CARD"
FILTER_SCHEMA[21]="FILTER_ONLY_OWL_MAX_Q_CARD"
FILTER_SCHEMA[22]="FILTER_ONLY_OWL_ON_CLASS"
FILTER_SCHEMA[23]="FILTER_ONLY_OWL_TYPE_CLASS"
FILTER_SCHEMA[24]="FILTER_ONLY_OWL_TYPE_DATATYPE_PROP"
FILTER_SCHEMA[25]="FILTER_ONLY_OWL_TYPE_OBJTYPE_TYPE"
FILTER_SCHEMA[26]="FILTER_ONLY_OWL_SAMEAS"

for (( i = 0 ; i < 27 ; i++ ))
do
echo "process schema ${SCHEMA_SUBSETS[$i]} ..."
mkdir "${CLOSURE_DIR}/${SCHEMA_SUBSETS[$i]}"
java -cp $CLASSPATH -Xmx256M jobs.ConvertFilesToPlainBinary ${1}/ ${CLOSURE_DIR}/${SCHEMA_SUBSETS[$i]} --outputType nl.vu.cs.querypie.storage.disk.PlainTripleFile --filter ${FILTER_SCHEMA[$i]}

done

echo "done"
