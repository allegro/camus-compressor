#!/usr/bin/env bash

# defaults
QUEUE="default"
EXECUTORS=2
COMPRESSION="snappy"

function usage {
  echo "Usage: compressor.sh [-c compression_format] [-e number_of_executors] [-q yarn_queue_name] -m mode -p path"
  echo "  Mode can be either all, topic or unit"
  echo "  Compression format can be either snappy, lzo or none"
  echo "  Default compression format is snappy"
  echo "  Default queue is \"default\""
  echo "  Default number of executors is 2"
  exit 1
}

while getopts ":q:e:m:p:" opt; do
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
    p)
      INPUTPATH=$OPTARG
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

if [ "$MODE" = "" -o "$INPUTPATH" = "" ]; then
  echo "Mode and path are required."
  usage
fi

spark-submit --class pl.allegro.tech.hadoop.compressor.Compressor \
  --queue $QUEUE \
  --master yarn-cluster \
  --num-executors $EXECUTORS \
  /usr/lib/hadoop-tools/camus-compressor/compressor.jar $MODE $INPUTPATH $COMPRESSION
