package com.ecoalis.minicluster.embedded.it;

import com.ecoalis.minicluster.core.ClusterRuntimeConfig;
import com.ecoalis.minicluster.embedded.testsupport.AbstractIntegrationTest;
import com.ecoalis.minicluster.embedded.testsupport.IntegrationTestSupport;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataFormatsIT extends AbstractIntegrationTest {

    @Override
    protected ClusterRuntimeConfig runtimeConfig() {
        java.util.Properties p = new java.util.Properties();
        p.setProperty("spark.enabled", "true");
        p.setProperty("sparkHive.enabled", "true");

        return ClusterRuntimeConfig.builder()
                .withHive()
                .withSpark()
                .props(p)
                .build();
    }

    @Test
    void spark_should_roundtrip_csv_to_parquet() throws Exception {
        // 1. On dépose un CSV d'entrée en HDFS
        IntegrationTestSupport.writeStringToHdfs(
                runtime.getFileSystem(),
                "/input/sales.csv",
                "country,amount\nFR,100\nFR,40\nDE,50\n"
        );

        // 2. On lit ce CSV via Spark
        Dataset<Row> dfCsv = spark().read()
                .option("header", "true")
                .csv(runtime.getHdfsUri() + "/input/sales.csv");

        long countCsv = dfCsv.count();

        // 3. On le réécrit en Parquet
        dfCsv.write()
                .mode("overwrite")
                .parquet(runtime.getHdfsUri() + "/output/sales_parquet");

        // 4. On relit le Parquet
        Dataset<Row> dfParquet = spark().read()
                .parquet(runtime.getHdfsUri() + "/output/sales_parquet");

        long countParquet = dfParquet.count();

        // 5. Sanity check : même volumétrie
        assertEquals(countCsv, countParquet,
                "Row count mismatch after CSV -> Parquet roundtrip");
    }
}