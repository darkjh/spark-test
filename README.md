spark-test
==========

Some hands-on tests with Spark

## Deployment ##
### Cluster ###
Do the following steps first:

  - Configure `spark-env.sh` in `conf/` folder
      + `JAVA_HOME`
	  + `SCALA_HOME`
	  + worker memory to use
  - Add all worker hostnames in `conf/slaves`
  - Then dispatch the configured Spark distribution to all worker nodes.
    They should have the same path.
  - When running, specify the memory usage for a job
    + `SPARK_MEM` as an env var
	+ or, `spark.executor.memory` in the program

Start the service using `bin/start-all.sh` in your master node.

By default,

  - Web UI: master-hostname:8080
  - Spark master url: master-hostname:7077

Launch jobs with `local` or `local[K]` in single machine mode, single thread or K threads respectively.

### Local cluster ###
It's also possible to run a spark master and multiple spark workers on a single machine. However some parameters need to be altered.

  - Setup the loopbacks:

    https://spark-project.atlassian.net/browse/SPARK-657

    ``` sh
	sudo ifconfig lo0 add 127.100.0.1
	sudo ifconfig lo0 add 127.100.0.2
	```

  - Start the master
    ``` sh
    ./run spark.deploy.master.Master -i localhost
	```
  - Start the workers
    ``` sh
    ./run spark.deploy.worker.Worker spark://localhost:7077 -c 1 -m 1G --webui-port 8081 -i 127.100.0.1
    ./run spark.deploy.worker.Worker spark://localhost:7077 -c 1 -m 1G --webui-port 8082 -i 127.100.0.2
	```
  - Start the driver program and point the master url at
    ``` sh
    MASTER=spark://localhost:7077 ./spark-shell
	```

### Amazon EC2 ###
...

## Compile ##

### Uber jar ###
Compile and package the project using `sbt-assembly`:

```sh
sbt assembly
```
the uber jar locates in `target/scala-VERSION` folder.

Be sure to configure the same `scalaVersion` as the scala version needed by Spark, or you will have some nasty merge conflicts when package the uber jar.

### Intellij IDEA ###
...
