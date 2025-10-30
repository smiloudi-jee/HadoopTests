package com.ecoalis.minicluster.modules.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.MiniHBaseCluster;

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

        Configuration conf = util.getConfiguration();

        // Clés cruciales pour éviter 2181 occupé et pertes de connexion
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        // Port aléatoire pour ZK embarqué (clé de test HBase)
        conf.setInt("test.hbase.zookeeper.property.clientPort", 0);
        // Evite la limite basse côté ZK sous charge de tests
        conf.setInt("hbase.zookeeper.property.maxClientCnxns", 0);
        // Znode parent explicite
        conf.set("zookeeper.znode.parent", "/hbase");

        // Timeouts un peu plus longs pour la stabilité locale
        conf.setInt("zookeeper.session.timeout", 30000);
        conf.setInt("zookeeper.recovery.retry", 3);
        conf.setInt("zookeeper.recovery.retry.intervalmill", 1000);

        // Lancement de Zookeeper
        util.startMiniZKCluster();

        StartMiniClusterOption option = StartMiniClusterOption.builder()
                .numMasters(1).numRegionServers(1).build();
        MiniHBaseCluster cluster = util.startMiniHBaseCluster(option);

        // Connexion client HBase
        Connection connection = ConnectionFactory.createConnection(util.getConfiguration());

        return new HBaseServiceHandle(conf, cluster, connection);
    }
}