package com.natixis.minicluster.embedded.it.job;

import com.natixis.minicluster.core.ClusterRuntimeConfig;
import com.natixis.minicluster.job.SalesAggregationJob;
import com.natixis.minicluster.testsupport.AbstractIntegrationTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test d'intégration Spark "métier".
 * <p>
 * On simule un flux réel :
 * * on dépose des données brutes dans HDFS
 * * on exécute le job Spark
 * * on lit le résultat du job
 * * on vérifie la logique business (somme des montants par pays)
 * <p>
 * → C'est exactement le pattern que les équipes doivent répliquer
 *    pour leurs propres traitements.
 */
public class SalesAggregationIT extends AbstractIntegrationTest {

    @Override
    protected ClusterRuntimeConfig runtimeConfig() {
        java.util.Properties p = new java.util.Properties();
        p.setProperty("spark.enabled", "true");
        p.setProperty("sparkHive.enabled", "true");
        // Hive est activé, car ton SparkSession est démarrée en mode Hive support
        return ClusterRuntimeConfig.builder()
                .withHive()
                .withSpark()
                .props(p)
                .build();
    }

    /**
     * Given : Préparer les données d'entrée
     * * On crée un CSV métier dans HDFS : /data/input/sales.csv
     * * FR:100 + FR:40 → FR total 140
     * * DE:50 → DE total 50
     * <p>
     * When : Exécuter le job métier Spark sur ces données
     * * On lui passe les paths complets HDFS (hdfs://host:port/...)
     * <p>
     * Then : Lire le résultat produit par le job
     * * Le job a écrit un CSV header=true dans /data/output/sales_agg
     * * Assertions métier → On s'attend à exactement 2 lignes
     */
    @Test
    public void shouldAggregateSalesAmountPerCountry() throws Exception {
        // Given : Preparation des données d'entrées
        String inputCsvPath = "/data/input/sales.csv";
        writeStringToHdfs(inputCsvPath, getDataEntries());

        // When : Exécuter le job métier Spark sur ces données
        String inputPathHdfs  = runtime.getHdfsUri() + "/data/input/sales.csv";
        String outputDirHdfs  = runtime.getHdfsUri() + "/data/output/sales_agg";
        SalesAggregationJob.run(spark(), inputPathHdfs, outputDirHdfs);

        // Then : Lire le résultat produit par le job
        Dataset<Row> resultDf = spark().read()
                .option("header", "true")
                .schema("country STRING, total_amount DOUBLE")
                .csv(runtime.getHdfsUri() + "/data/output/sales_agg");

        // Petit affichage debug en mode dev (ne gêne pas les assertions)
        resultDf.show(false);

        // On collecte le résultat pour assertions => [("DE",50.0), ("FR",140.0)]
        List<Row> rows = resultDf.collectAsList();

        // Assertions métier
        assertEquals(2, rows.size(), "Expected 2 countries in aggregation result");

        // Ici, on fait simple : on recherche FR et DE dans la liste
        Double totalFR = null;
        Double totalDE = null;

        for (Row r : rows) {
            String country = r.getAs("country");
            Double total   = r.getAs("total_amount");
            if ("FR".equals(country)) {
                totalFR = total;
            } else if ("DE".equals(country)) {
                totalDE = total;
            }
        }

        assertNotNull(totalFR, "FR should be present in result");
        assertNotNull(totalDE, "DE should be present in result");
        // On vérifie les totaux pays par pays
        assertEquals(140.0, totalFR, 0.0001, "FR total_amount mismatch");
        assertEquals(50.0, totalDE, 0.0001, "DE total_amount mismatch");
    }

    private String getDataEntries(){
        return "country,amount\n" +
                "FR,100\n" +
                "FR,40\n" +
                "DE,50\n";
    }
}