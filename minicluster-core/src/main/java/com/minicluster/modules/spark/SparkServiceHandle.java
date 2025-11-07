package com.minicluster.modules.spark;


import org.apache.spark.sql.SparkSession;

public final class SparkServiceHandle implements AutoCloseable {

    private final SparkSession sparkSession;

    public SparkServiceHandle(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public SparkSession sparkSession() {
        return sparkSession;
    }

    public void stop() {
        sparkSession.stop();
    }

    @Override
    public void close() {
        stop();
    }
}
