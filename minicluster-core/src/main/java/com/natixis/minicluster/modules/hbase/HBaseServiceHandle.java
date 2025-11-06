package com.natixis.minicluster.modules.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Connection;

public class HBaseServiceHandle implements AutoCloseable {

    private final Configuration configuration;
    private final MiniHBaseCluster cluster;
    private final Connection connection;

    public HBaseServiceHandle(Configuration configuration, MiniHBaseCluster cluster, Connection connection) {
        this.configuration = configuration;
        this.cluster = cluster;
        this.connection = connection;
    }

    @Override
    public void close() {
        try { connection.close(); } catch (Exception ignored) {}
        try { cluster.shutdown(); } catch (Exception ignored) {}
        try { cluster.close(); } catch (Exception ignored) {}
    }

    public Configuration getConfiguration() { return configuration; }
    public MiniHBaseCluster getCluster() { return cluster; }
    public Connection getConnection() { return connection; }
}