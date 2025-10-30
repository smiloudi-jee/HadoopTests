package com.ecoalis.minicluster.modules.mapreduce;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration MapReduce en "mode local" pour les tests
 * * Exécutions des jobs MR dans la JVM courante,
 * * tout en lisant/écrivant sur le HDFS du MiniCluster.
 * <p>
 * => Pas de YARN, compatible Windows.
 */
public final class MapReduceBootstrap {

    private MapReduceBootstrap() {}

    public static MapReduceServiceHandle startLocalMapReduce(Configuration baseHadoopConf, String defaultFsUri) {
        Configuration mrConf = new Configuration(baseHadoopConf);

        // Force le mode MR local
        mrConf.set("mapreduce.framework.name", "local");

        // Set les FS par défaut sur le HDFS du MiniCluster
        mrConf.set("fs.defaultFS", defaultFsUri);
        mrConf.set("mapreduce.jobtracker.address", "local");

        return new MapReduceServiceHandle(mrConf);
    }
}
