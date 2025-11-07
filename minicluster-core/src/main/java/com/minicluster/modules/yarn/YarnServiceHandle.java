package com.minicluster.modules.yarn;

import org.apache.hadoop.yarn.server.MiniYARNCluster;

/**
 * Handle léger autour du MiniYARNCluster.
 * Permet de l'arrêter proprement via ClusterRuntime.close().
 */
public class YarnServiceHandle implements AutoCloseable {

    private final MiniYARNCluster yarnCluster;

    public YarnServiceHandle(MiniYARNCluster yarnCluster) {
        this.yarnCluster = yarnCluster;
    }

    public MiniYARNCluster getYarnCluster() {
        return yarnCluster;
    }

    @Override
    public void close() {
        try {
            yarnCluster.stop();
        } catch (Exception ignored) {
        }
    }
}