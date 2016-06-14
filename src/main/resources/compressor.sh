#!/usr/bin/env bash

# defaults
QUEUE="default"
EXECUTORS=2
DELAY=2
COMPRESSION="snappy"
FORMAT="json"
SCHEMAREPOSITORYURL=""

function usage {
  echo "Usage: compressor.sh [-c compression_format] [-e number_of_executors] [-q yarn_queue_name] [-f format=json] -m mode -d delay_in_days -p path"
  echo "  Mode can be either all, topic or unit"
  echo "  Compression format can be either snappy, lzo or none"
  echo "  Default compression format is snappy"
  echo "  Default queue is \"default\""
  echo "  Default number of executors is 2"
  echo "  Default number of days for compression delay is 2"
  echo "  Default format is \"json\". Another available option is \"avro\""
  echo "  When format==\"avro\" then you must provide URL to the instance of schema-repo (http://github.com/schema-repo/schema-repo)"
  exit 1
}

while getopts ":q:e:c:m:d:p:s:f:" opt; do
  case $opt in
    q)
      QUEUE=$OPTARG
      ;;
    e)
      EXECUTORS=$OPTARG
      ;;
    c)
      COMPRESSION=$OPTARG
      ;;
    m)
      MODE=$OPTARG
      ;;
    d)
      DELAY=$OPTARG
      ;;
    p)
      INPUTPATH=$OPTARG
      ;;
    s)
      SCHEMAREPOSITORYURL="$OPTARG"
      ;;
    f)
      FORMAT=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG"
      usage
      ;;
    :)
      echo "Option -$OPTARG requires an argument."
      usage
      ;;
  esac
done

if [ "$FORMAT" != "json" -a "$FORMAT" != "avro" ]; then
    echo "Invalid option format: $FORMAT"
    usage
fi

if [ "$MODE" = "" -o "$INPUTPATH" = "" ]; then
  echo "Mode and path are required."
  usage
fi

if [ "$FORMAT" == "avro" -a "$SCHEMAREPOSITORYURL" == "" ]; then
    echo "Please provide -s http://schema.repository.url/"
    usage
fi

spark-submit1.6 --class pl.allegro.tech.hadoop.compressor.Compressor \
  --queue $QUEUE \
  --master yarn-cluster \
  --num-executors $EXECUTORS \
  /usr/lib/hadoop-tools/camus-compressor/compressor.jar $MODE $INPUTPATH $COMPRESSION $DELAY $FORMAT $SCHEMAREPOSITORYURL
