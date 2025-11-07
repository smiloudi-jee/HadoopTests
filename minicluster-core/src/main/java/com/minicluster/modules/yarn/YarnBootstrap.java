package com.minicluster.modules.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

import java.io.IOException;

/**
 * Démarre un MiniYARNCluster (ResourceManager + NodeManager)
 * pour permettre de tester des jobs YARN / MapReduce dans le MiniCluster.
 */
public final class YarnBootstrap {

    private YarnBootstrap() {}

    public static YarnServiceHandle startYarn(Configuration baseHadoopConf) throws IOException {
        baseHadoopConf.set("hadoop.tmp.dir", "file:/tmp/hadoop-test");
        baseHadoopConf.set("yarn.nodemanager.local-dirs", "file:/tmp/hadoop-test/nm-local-dir");
        baseHadoopConf.set("yarn.nodemanager.log-dirs", "file:/tmp/hadoop-test/logs");
        baseHadoopConf.set("mapreduce.cluster.local.dir", "file:/tmp/hadoop-test/mapred-local");
        // MiniYARNCluster prend (nom, numResourceManagers, numNodeManagers, numLocalDirsPerNM)
        MiniYARNCluster yarnCluster = new MiniYARNCluster(
                "mini-yarn",
                1,
                1,
                1
        );

        // On injecte la conf Hadoop existante (celle qu'on utilise déjà pour HDFS).
        yarnCluster.init(baseHadoopConf);
        yarnCluster.start();

        return new YarnServiceHandle(yarnCluster);
    }
}