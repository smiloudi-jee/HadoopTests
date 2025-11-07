package com.minicluster.modules.hive;

import com.minicluster.util.HadoopConstants;
import com.minicluster.util.StarterUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Properties;

import static com.minicluster.util.HadoopConstants.*;

public final class HiveBootstrap {

    private HiveBootstrap() {}

    public static HiveServiceHandle startHive(Properties props,
                                              String hdfsUri,
                                              FileSystem fs,
                                              java.nio.file.Path workDir) throws IOException {

        // 1. Warehouse HDFS
        final String hiveWarehousePath = props.getProperty(HIVE_WAREHOUSE_DIR,
                HadoopConstants.DEFAULT_HIVE_WAREHOUSE_DIR);
        fs.mkdirs(new Path(hiveWarehousePath));

        // 2. Metastore Derby local
        final java.nio.file.Path metaDir = workDir.resolve(HadoopConstants.METASTORE_DIR_NAME);
        StarterUtils.ensureDirectory(metaDir);

        if (Boolean.parseBoolean(props.getProperty(METASTORE_CLEAN_ON_START, "false"))) {
            StarterUtils.deleteRecursively(metaDir);
            StarterUtils.ensureDirectory(metaDir);
        }

        // Derby va écrire là
        System.setProperty(DERBY_SYSTEM_HOME, metaDir.toString());

        return new HiveServiceHandle(hiveWarehousePath);
    }
}