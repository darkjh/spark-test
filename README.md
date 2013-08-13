spark-test
==========

Some hands-on tests with Spark

## Deployment ##
### Cluster ###
Do the following steps first:

  - Configure `spark-env.sh` in `conf/` folder
      + JAVA_HOME
	  + SCALA_HOME
	  + memory limite
  - Add all workers in `conf/slaves`

Then dispatch the configured Spark distribution to all worker nodes. They should have the same path.

Start the service using `bin/start-all.sh` in your master node.

By default,

  - Web UI: master-hostname:8080
  - Spark master url: master-hostname:7077

Launch jobs with `local` or `local[K]` in single machine mode, single thread or K threads respectively.

### Amazon EC2 ###
...

## Compile ##
Compile and package the project using `sbt-assembly`:

```sh
sbt assembly
```
the uber jar locates in `target/scala-version` folder.

Be sure to configure the same `scalaVersion` as the scala version needed by Spark, or you will have some nasty merge conflicts when package the uber jar.
