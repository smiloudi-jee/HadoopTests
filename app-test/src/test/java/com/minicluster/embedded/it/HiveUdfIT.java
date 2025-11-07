package com.minicluster.embedded.it;

import com.minicluster.core.ClusterRuntimeConfig;
import com.minicluster.testsupport.AbstractIntegrationTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HiveUdfIT extends AbstractIntegrationTest {

    @Override
    protected ClusterRuntimeConfig runtimeConfig() {
        java.util.Properties p = new java.util.Properties();
        p.setProperty("spark.enabled", "true");
        p.setProperty("sparkHive.enabled", "true");
        // Pas besoin du REST ici
        // Pas besoin du Thrift ici pour le test UDF pur

        return ClusterRuntimeConfig.builder()
                .withHive()
                .withSpark()
                .props(p)
                .build();
    }

    @Test
    void to_upper_udf_should_transform_string() {
        // On exécute une requête SQL qui appelle UDF enregistrée
        Dataset<Row> df = runSql(
                spark(),
                "SELECT to_upper('france') AS val"
        );

        // On récupère la colonne "val" pour inspection
        List<String> values = collectColumn(df, "val");

        // On veut voir la version upper
        assertTrue(values.contains("FRANCE"),
                "Expected 'FRANCE' from to_upper('france'), got: " + values);
    }
}