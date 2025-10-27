package com.ecoalis.minicluster.modules.hive;

/**
 * Wrapper du service Hive (warehouse + éventuellement HiveThriftServer2).
 */
public final class HiveServiceHandle implements AutoCloseable {

    private final String warehouseHdfsPath;

    public HiveServiceHandle(String warehouseHdfsPath) {
        this.warehouseHdfsPath = warehouseHdfsPath;
    }

    public String getWarehouseHdfsPath() {
        return warehouseHdfsPath;
    }

    public void stop() {
        // rien de spécifique pour l’instant (le Thrift est géré côté SparkBootstrap)
    }

    @Override
    public void close() {
        stop();
    }
}