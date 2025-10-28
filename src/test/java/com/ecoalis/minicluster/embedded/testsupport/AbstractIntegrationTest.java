package com.ecoalis.minicluster.embedded.testsupport;

import com.ecoalis.minicluster.core.ClusterRuntime;
import com.ecoalis.minicluster.core.ClusterRuntimeConfig;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIntegrationTest {

    protected ClusterRuntime runtime;

    @BeforeAll
    void beforeAll() throws Exception {
        runtime = ClusterRuntime.start(runtimeConfig());
    }

    @AfterAll
    void afterAll() {
        if (runtime != null) {
            runtime.close();
        }
    }

    /**
     * Chaque test concret doit définir quels services il veut :
     * - only HDFS     => builder() sans withSpark/withHive/withRest
     * - Spark/Hive    => withSpark().withHive()
     * - REST aussi    => withRest()
     * - Thrift serveur JDBC => withThrift()
     */
    protected abstract ClusterRuntimeConfig runtimeConfig();

    /**
     * Helper rapide : écrire un string dans HDFS.
     * Tu peux aussi décider de déléguer à IntegrationTestSupport.writeStringToHdfs(...)
     */
    protected void writeStringToHdfs(String hdfsPath, String content) throws Exception {
        FileSystem fs = runtime.getFileSystem();
        Path p = new Path(hdfsPath);

        if (!fs.exists(p.getParent())) {
            fs.mkdirs(p.getParent());
        }
        try (FSDataOutputStream out = fs.create(p, true)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Accès Spark depuis un test qui a activé withSpark() dans runtimeConfig().
     */
    protected SparkSession spark() {
        return runtime.getSparkSession()
                .orElseThrow(() ->
                        new IllegalStateException("Spark not enabled for this test runtime"));
    }
}