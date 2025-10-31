package com.ecoalis.minicluster.modules.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.MiniHBaseCluster;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Démarre un mini-cluster HBase embarqué (HBase 2.5.12-hadoop3).
 * Hypersimple et robuste : 1 master, 1 regionserver, ZK/HDFS internes.
 */
public final class HBaseBootstrap {

    private HBaseBootstrap() {}

    public static HBaseServiceHandle startHBase() throws Exception {
        HBaseTestingUtility util = new HBaseTestingUtility();

        // Nettoyage des répertoires de test d’un run précédent
        try { util.cleanupTestDir(); } catch (Exception ignore) {}

        Configuration conf = buildConfiguration(util.getConfiguration());

        // Lancement de Zookeeper
        util.startMiniZKCluster();

        StartMiniClusterOption option = StartMiniClusterOption.builder()
                .numMasters(1).numRegionServers(1).build();
        MiniHBaseCluster cluster = util.startMiniHBaseCluster(option);

        // Connexion client HBase
        Connection connection = ConnectionFactory.createConnection(util.getConfiguration());

        return new HBaseServiceHandle(conf, cluster, connection);
    }

    private static Configuration buildConfiguration(Configuration configuration){
        // 🔀 Force un dataDir dédié pour ZK/HBase dans ton répertoire target
        Path baseDir = Paths.get("target", "hbase-minicluster");
        configuration.set("hbase.rootdir", baseDir.resolve("hbase-root").toUri().toString());
        configuration.set("hbase.zookeeper.property.dataDir", baseDir.resolve("zk-data").toString());

        // Clés cruciales pour éviter 2181 occupé et pertes de connexion
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        // Port aléatoire pour ZK embarqué (clé de test HBase)
        configuration.setInt("test.hbase.zookeeper.property.clientPort", 0);
        // Evite la limite basse côté ZK sous charge de tests
        configuration.setInt("hbase.zookeeper.property.maxClientCnxns", 0);
        // Znode parent explicite
        configuration.set("zookeeper.znode.parent", "/hbase");

        // Timeouts un peu plus longs pour la stabilité locale
        configuration.setInt("zookeeper.session.timeout", 30000);
        configuration.setInt("zookeeper.recovery.retry", 3);
        configuration.setInt("zookeeper.recovery.retry.intervalmill", 1000);

        return configuration;
    }
}