# Camus Compressor

[![Build Status](https://travis-ci.org/allegro/camus-compressor.svg?branch=master)](https://travis-ci.org/allegro/camus-compressor)

Camus Compressor merges files created by [Camus]( https://github.com/linkedin/camus) 
and saves them in a compressed format.

Camus is massively used at Allegro for dumping more than 200 Kafka topics onto HDFS. 
The script runs every 15 minutes and creates one file per Kafka partition which results 
in about 76800 small files per day. Most of the files do not exceed Hadoop block size. 
This is a clear Hadoop antipattern which leads to performance issues, for example 
extensive number of mappers in SQL queries’ executions.

Camus Compressor solves this issue by merging files within Hive partition and compressing 
them. It does not change Camus directories structure and supports well daily and hourly 
partitioning. The tool runs in YARN and is build on [Spark](https://github.com/apache/spark).

## Supported compressors

 * Snappy
 * LZO (needs extra [hadoop-lzo](https://github.com/twitter/hadoop-lzo) package on nodes)
 * Deflate

## How to use

As mentioned above You need [Spark](https://github.com/apache/spark) packages to run 
Camus Compressor. Provided `src/main/resources/compressor.sh` file helps executing 
`spark-submit` commands by setting options:

 * `c`: Compression format. Could be: `deflate`, `lzo`, `snappy` or `none` (default: `snappy`)
 * `q`: YARN queue name (default: `default`)
 * `e`: Parrallelism level of task (number of spark executors, default: 2)
 * `p`: Input path
 * `m`: Input mode, can be either :
     * `all`: compress all camus dir, put main camus dir as input path
     * `topic`: compress only one topic, put topic dir as the input path
     * `unit`: compress low-level camus directory (hour dir on hourly patitioning 
       and day dir on daily partitioning), put appropriate input path to unit
 * `d`: Compression delay, in days (compress data older than given number of days, default: 2)
 * `f`: Input files format - `json` or `avro`
 * `s`: When your input files are in `avro` format you need to pass an URL to the 
 [schema-repo][https://github.com/schema-repo/schema-repo] instance, e.g. `-s https://schemarepo.example.com/schema-repo`.

By default Camus Compressor doesn't compress directories again, to force recompression add `--force` flag to `spark-submit`.

Compressor assumes that Camus files are stored in a directory with the names equal to `kafkaTopic.replace(".", "_")`
with optional `_avro` suffix. 

## How to build

Camus Compressor is shipped as fatjar file or Debian package and build using Gradle. 
To build fat-jar run in `build/libs/`:
    
    ./gradlew shadowJar
    
To build debian package `camus-compressor` in `build/`, run:

    ./gradlew shadowJar prepareControlFile buildDeb

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
