package com.ecoalis.minicluster.testsupport;
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

    protected abstract ClusterRuntimeConfig runtimeConfig();

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

    protected SparkSession spark() {
        return runtime.getSparkSession()
                .orElseThrow(() ->
                        new IllegalStateException("Spark not enabled for this test runtime"));
    }
}