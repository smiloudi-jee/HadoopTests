package com.minicluster.embedded.it;

import com.minicluster.core.ClusterRuntimeConfig;
import com.minicluster.testsupport.AbstractIntegrationTest;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterRuntimeHdfsIT extends AbstractIntegrationTest {

    /**
     *  Test environnement Hadoop de base
     *  * HDFS activé par défault
     *  * On active le basic : Hive, Spark, REST, Thrift JDBC
     */
    @Override
    protected ClusterRuntimeConfig runtimeConfig() {
        java.util.Properties p = new java.util.Properties();
        p.setProperty("rest.enabled", "true");
        p.setProperty("spark.enabled", "true");
        p.setProperty("sparkHive.enabled", "true");
        p.setProperty("thrift.enabled", "true");

        return ClusterRuntimeConfig.builder()
                .withHive()
                .withSpark()
                .withRest()
                .withThrift()
                .props(p)
                .build();
    }

    @Test
    void cluster_should_start_and_expose_all_services() throws Exception {

        // 1. HDFS accessible
        assertNotNull(runtime.getFileSystem(), "FileSystem should not be null");
        assertTrue(runtime.getFileSystem().exists(new Path("/")), "HDFS root should exist");

        // 2. On peut écrire un fichier dans HDFS
        writeStringToHdfs("/tmp/check.txt", "hello world");
        assertTrue(runtime.getFileSystem().exists(new Path("/tmp/check.txt")),
                "Written file should exist in HDFS");

        // 3. Spark accessible
        assertTrue(runtime.getSparkSession().isPresent(), "Spark should be enabled in this runtime");
        SparkSession spark = runtime.getSparkSession().get();

        Dataset<Row> df = spark.sql("SELECT 1 AS ok");
        assertEquals(1L, df.count(), "Spark SQL basic query should return 1 row");

        // 4. REST exposé ?
        assertTrue(runtime.getRestHandle().isPresent(), "REST should be enabled in this runtime");
        int restPort = runtime.getRestHandle().get().getPort();
        assertTrue(restPort > 0, "REST port should be > 0");
    }
}