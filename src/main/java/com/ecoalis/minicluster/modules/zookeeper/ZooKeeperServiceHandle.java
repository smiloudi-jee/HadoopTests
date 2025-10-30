package com.ecoalis.minicluster.modules.zookeeper;

import org.apache.curator.test.TestingServer;

/**
 * Handle pour un ZooKeeper embarqué.
 */
public class ZooKeeperServiceHandle implements AutoCloseable {

    private final TestingServer server;

    public ZooKeeperServiceHandle(TestingServer server) {
        this.server = server;
    }

    public String getConnectString() {
        return server.getConnectString(); // exemple: "127.0.0.1:21812"
    }

    @Override
    public void close() {
        try {
            server.close();
        } catch (Exception ignored) {}
    }
}