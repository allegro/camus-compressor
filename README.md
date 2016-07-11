# Camus Compressor

[![Build Status](https://travis-ci.org/allegro/camus-compressor.svg?branch=master)](https://travis-ci.org/allegro/camus-compressor)

Camus Compressor merges files created by [Camus][3] 
and saves them in a compressed format.

Camus is massively used at Allegro for dumping more than 200 Kafka topics onto HDFS. 
The script runs every 15 minutes and creates one file per Kafka partition which results 
in about 76800 small files per day. Most of the files do not exceed Hadoop block size. 
This is a clear Hadoop antipattern which leads to performance issues, for example 
extensive number of mappers in SQL queries’ executions.

Camus Compressor solves this issue by merging files within Hive partition and compressing 
them. It does not change Camus directories structure and supports well daily and hourly 
partitioning. The tool runs in YARN and is build on [Spark][4].

## Supported compressors

 * Snappy
 * LZO (needs extra [hadoop-lzo][2] package on nodes)
 * Deflate
 * None (without compression, files are compacted only).

## How to use

As mentioned above You need [Spark][4] packages to run 
Camus Compressor. Provided `src/main/resources/compressor.sh` file helps executing 
`spark-submit` commands by setting options:
 
 * `P`: Configuration file path (default: `/etc/camus-compressor/camus-compressor.properties`);
 * `q`: YARN queue name (default: `default`);
 * `e`: Parrallelism level of task (number of spark executors, default: 2);
 * `m`: Spark `--master` option;
 * `c`: Spark `--conf` option. You can use it to pass extra arguments to job.
 * `d`: Spark `--driver-memory` option. If you have huge number of partitions to compress, please consider set this 
   option to, for example, `4g`.
 
## Configuration file

In a configuration file (`/etc/camus-compressor/camus-compressor.properties`) you can set following options:
 
 * `spark.compressor.input.format` - input files format (`avro` or `json`)
 * `spark.compressor.processing.mode` - processing mode, can be either:
      * `all`: compress all camus dir, put main camus dir as input path
      * `topic`: compress only one topic, put topic dir as the input path
      * `unit`: compress low-level camus directory (hour dir on hourly patitioning 
        and day dir on daily partitioning), put appropriate input path to unit
 * `spark.compressor.input.path` - path to the uncompressed file
      * in `all` mode is a path to the directory containing topics that should be compressed (`/user/username/topics`)
      * in `topic` mode is a path to the topic directory (`/user/username/topics/my_topic`)
      * in `unit` mode is a path to one partition (directory containing files: 
        `/user/username/topics/my_topic/daily/2016/03/23/`)
 * `spark.compressor.output.compression` - output compression (`snappy`, `deflate`, `none`, `lzo`).
 * `spark.compressor.avro.schema.repository.class` - currently only [schema-repo][1] is supported. Use 
   `pl.allegro.tech.hadoop.compressor.schema.SchemaRepoSchemaRepository` as a value.
 * `spark.compressor.avro.schema.repository.url` - URL to the schema-repo, for example: `http://example.com/schema-repo`.
 * `spark.compressor.processing.topic-name-retriever.class`:
      * use `pl.allegro.tech.hadoop.compressor.schema.KafkaTopicNameRetriever` if directories on HDFS are named after
        Kafka topics with dots replaced with underscores (`topic.name` on Kafka would be `topic_name` on HDFS).
      * use `pl.allegro.tech.hadoop.compressor.schema.IdentityTopicNameRetriever` if directories on HDFS have names 
        identical to topic names.
 * `spark.compressor.zookeeper.paths` - when using `KafkaTopicNameRetriever`, please provide Zookeeper connection string.
 * `spark.compressor.processing.delay` - compression delay, in days (compress data older than given number of days, default: 2).
 * `spark.compressor.processing.mode.all.excludes` - comma separated list of directories excluded from compression.
    If you want to blacklist some topics, you can pass their names here.
 * `spark.compressor.processing.mode.all.timeout.minutes` - after this period of time Compressor will be termineted. 
 * `spark.compressor.processing.mode.topic.pattern` - comma separated list of glob patterns of directories that need to 
   be compressed. For Camus you can use `hourly/*/*/*/*,daily/*/*/*`. 
 * `spark.compressor.processing.force` - by default Camus Compressor doesn't compress directories again. Set this 
   option to `true` to force recompression. 
 * `spark.compressor.processing.working.dir` - directory where temprary files will be stored.
 * `spark.compressor.processing.calculate.counts` - if set to `true`, Compressor will calculate counts for every unit
   (i.e. partition) before and after compression. If they are not equal, it will throws an exception and leaves
   compressed files in `working.dir`.
 * `spark.executors.instances` - number of topics concurrently processed.
 
## How to build

Camus Compressor is shipped as fatjar file or Debian package and build using Gradle. 
To build fat-jar run in `build/libs/`:
    
    ./gradlew shadowJar
    
To build debian package `camus-compressor` in `build/`, run:

    ./gradlew shadowJar prepareControlFile buildDeb

## Sample usage

Before executing `compressor.sh` script, make sure that `SPARK_SUBMIT` variable is set to the `spark-submit` location
of Spark in version 1.6.1 or above.

    export SPARK_SUBMIT="/usr/bin/spark-submit"
    compressor.sh -P /etc/camus-compressor/camus-compressor.properties \
        -e 10 \
        -q default \
        -d 4g \
        -m yarn-cluster \
        -c "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"

## License

Copyright 2015 Allegro Group

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[1]: https://github.com/schema-repo/schema-repo
[2]: https://github.com/twitter/hadoop-lzo
[3]: https://github.com/linkedin/camus
[4]: https://github.com/apache/spark