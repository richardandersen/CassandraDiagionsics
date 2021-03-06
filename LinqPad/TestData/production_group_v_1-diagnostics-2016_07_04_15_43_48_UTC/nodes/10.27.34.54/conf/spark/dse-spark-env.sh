#!/bin/sh

if [ "$SPARK_WORKER_ORDINAL_NUMBER" = "" ]; then
    SPARK_WORKER_ORDINAL_NUMBER="0"
fi

workerSubDir="worker-$SPARK_WORKER_ORDINAL_NUMBER"

if [ "$SPARK_CASSANDRA_CONNECTION_HOST" ]; then
    SPARK_CASSANDRA_CONNECTION_HOST_PARAM="-Dspark.cassandra.connection.host=$SPARK_CASSANDRA_CONNECTION_HOST"
else
    SPARK_CASSANDRA_CONNECTION_HOST_PARAM="-Dspark.cassandra.connection.host=$CASSANDRA_ADDRESS"  # defined in dse.in.sh
fi

if [ "$SPARK_DRIVER_HOST" ]; then
    SPARK_DRIVER_HOST_PARAM="-Dspark.driver.host=$SPARK_DRIVER_HOST"
else
    SPARK_DRIVER_HOST_PARAM=""
fi

log_config() {
    result="-Dlogback.configurationFile=$SPARK_CONF_DIR/$1"
    if [ "$2" != "" ] && [ "$3" != "" ]; then
        result="$result -Dspark.log.dir=$2 -Dspark.log.file=$3"
    fi
    echo "$result"
}

# Library paths... not sure whether they are required for
# TODO consider using LD_LIBRARY_PATH or DYLD_LIBRARY_PATH env variables
SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"

# Memory options
export SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS -XX:MaxHeapFreeRatio=50 -XX:MinHeapFreeRatio=20"  # don't use too much memory
export SPARK_DAEMON_MEMORY=256M

# Set library paths for Spark daemon process as well as to be inherited by executor processes
if [ "$(echo "$OSTYPE" | grep "^darwin")" != "" ]; then
    # For MacOS...
    export DYLD_LIBRARY_PATH="$JAVA_LIBRARY_PATH"
else
    # For any other Linux-like OS
    export LD_LIBRARY_PATH="$JAVA_LIBRARY_PATH"
fi

export SPARK_COMMON_OPTS=" -Dspark.kryoserializer.buffer.mb=10 $DSE_OPTS "

export SPARK_WORKER_OPTS="$SPARK_WORKER_OPTS $SPARK_COMMON_OPTS "
export SPARK_WORKER_OPTS="$SPARK_WORKER_OPTS $(log_config "logback-spark-server.xml" "$SPARK_WORKER_LOG_DIR/$workerSubDir" "worker.log") "

export SPARK_EXECUTOR_OPTS="$SPARK_EXECUTOR_OPTS $SPARK_COMMON_OPTS "
export SPARK_EXECUTOR_OPTS="$SPARK_EXECUTOR_OPTS $(log_config "logback-spark-executor.xml") "
export SPARK_EXECUTOR_OPTS="$SPARK_EXECUTOR_OPTS -Ddse.client.configuration.impl=com.datastax.bdp.transport.client.HadoopBasedClientConfiguration "

export SPARK_DRIVER_OPTS="$SPARK_DRIVER_OPTS -Ddse.client.configuration.impl=com.datastax.bdp.transport.client.HadoopBasedClientConfiguration "

export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS $SPARK_COMMON_OPTS $SPARK_CASSANDRA_CONNECTION_HOST_PARAM "
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS $(log_config "logback-spark.xml") "
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS $SPARK_DRIVER_HOST_PARAM "
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS $SPARK_DRIVER_OPTS "

export SPARK_WORKER_DIR="$SPARK_WORKER_DIR/$workerSubDir"
