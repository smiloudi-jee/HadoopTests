package com.natixis.minicluster.modules.zookeeper;

import org.apache.curator.test.TestingServer;

/**
 * Démarre un ZooKeeper embarqué pour les tests.
 * Utilisé notamment par HBase en mode mini-cluster.
 */
public final class ZooKeeperBootstrap {

    private ZooKeeperBootstrap() {}

    public static ZooKeeperServiceHandle startZookeeper() throws Exception {
        // port = 0 → port aléatoire libre
        // true = delete dataDir, on close
        TestingServer server = new TestingServer(true);
        return new ZooKeeperServiceHandle(server);
    }
}
