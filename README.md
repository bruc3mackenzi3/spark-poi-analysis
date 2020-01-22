# EQWorks Data Engineer

## Solution
From root directory:
`docker-compose up -d`

Access Spark UI:
http://localhost:8080

Attach to Docker Container:
`docker exec -it eqworks-spark_master_1 /bin/bash`

## Problem Description
[https://gist.github.com/woozyking/f1d50e1fe1b3bf52e3748bc280cf941f](https://gist.github.com/woozyking/f1d50e1fe1b3bf52e3748bc280cf941f)

## Part 1 - Spark POI Analysis
### Spark + Docker Environment
[https://github.com/EQWorks/ws-data-spark](https://github.com/EQWorks/ws-data-spark)

## Part 2 - DAG Data Analysis Pipeline
## Submission
* Link to git repo
* `git-archive` of my implementation / the repo?


## Environment

Unless you already have a working [Apache Spark](http://spark.apache.org/) cluster, you will need to have [Docker](https://docs.docker.com/) for simple environment setup.

The provided `docker-compose.yml` and Spark configurations in `conf` directory are cloned from <https://github.com/gettyimages/docker-spark>.

## Setup

0. Make sure Docker is installed properly and `docker-compose` is ready to use
1. Run `$ docker-compose up -d` under the `data-mr` directory
2. Check Spark UI at `http://localhost:8080` and you should see 1 master and 1 worker
3. Run `$ docker exec -it eqworks-spark_master_1 /bin/bash` to get into the container shell, and start utilizing Spark commands such as `# spark-shell`, `# pyspark` or `# spark-submit`. You may want to replace `datamr_master_1` with actual container name that's spawned by the `docker-compose` process

![demo.gif](https://user-images.githubusercontent.com/2837532/27649289-4fdffd52-5bff-11e7-9236-0a1d063461cb.gif)

## Notes on working through the problems

If you're not already familiar with [Apache Spark](http://spark.apache.org/), you'll need to go through its documentation for available APIs. The version that comes with the Docker Spark setup depends on https://github.com/gettyimages/docker-spark.

For jobs that rely on external dependencies and libraries, make sure they are properly packaged on submission.

On submission, we will need:

1. Source code of the solution
2. Build instructions for job packaging (unless your solution is a single `.py`), such as [Maven](https://maven.apache.org/) or [SBT](http://www.scala-sbt.org/) for Scala/Java, or `setup.py` for Python `.zip/.egg`

Make sure the jobs can be submitted (through `spark-submit` command) in the Spark Master container shell. There is a `data` directory provided that maps between the Spark Master container and your host system, which is accessible as `/tmp/data` within the Docker container -- this is where you want to place both your jobs and work sample data, the latter is already included.
