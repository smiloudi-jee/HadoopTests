package com.ecoalis.minicluster.embedded.it.job;

import com.ecoalis.minicluster.core.ClusterRuntimeConfig;
import com.ecoalis.minicluster.job.ClientRiskCheckSparkJob;
import com.ecoalis.minicluster.embedded.testsupport.AbstractIntegrationTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test d'intégration contractuelle "provider → client".
 * <p>
 * But :
 * * On simule l'appli provider en écrivant des données figées dans HDFS.
 * * On exécute le job du client, qui consomme ces données comme si elles venaient du provider réel.
 * * On vérifie que le client interprète correctement ces données :
 * * calcul d'exposition, détection de dépassement de seuil, etc.
 * <p>
 * Si ce test casse, ça veut dire :
 * * le format provider a changé
 * * la logique client n'est plus compatible,
 * * ou quelqu'un a cassé le contrat d'intégration.
 * <p>
 * → C'est un garde-fou d'intégration "chaîne".
 */
public class ProviderToClientContractIT extends AbstractIntegrationTest {

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

    /**
     *  Given : Le provider "publie" un snapshot dans HDFS
     *  * On écrit dans /provider/positions.csv un échantillon mini, mais réaliste.
     *  * Ici desk EQUITIES dépasse le seuil (par ex 120M > limite 100M),
     *  * CREDIT reste sous le seuil.
     *  *
     *  When : Le client consomme ces données
     *  *
     *  Then : On lit la sortie calculée par le client
     */
    @Test
    public void clientShouldLabelDesksBreachingLimitFromProviderSnapshot() throws Exception {
        // Given : Le provider "publie" un snapshot dans HDFS
        String providerInput = "/provider/positions.csv";
        writeStringToHdfs(providerInput, getData());

        // When : Le client consomme ces données
        String inputPathHdfs  = runtime.getHdfsUri() + "/provider/positions.csv";
        String outputPathHdfs = runtime.getHdfsUri() + "/client/exposure_check";
        double limitThreshold = 100.0; // seuil limite interne définie par le métier
        ClientRiskCheckSparkJob.run(spark(), inputPathHdfs, outputPathHdfs, limitThreshold);

        // Then : On lit la sortie calculée par le client
        Dataset<Row> resultDf = spark().read()
                .option("header", "true")
                .schema("desk STRING, total_notional DOUBLE, limit_breached BOOLEAN")
                .csv(runtime.getHdfsUri() + "/client/exposure_check");

        List<Row> rows = resultDf.collectAsList();

        Double equitiesTotal = null;
        Boolean equitiesBreach = null;
        Double creditTotal = null;
        Boolean creditBreach = null;

        for (Row r : rows) {
            String desk = r.getAs("desk");
            Double total = r.getAs("total_notional");
            Boolean breach = r.getAs("limit_breached");

            if ("EQUITIES".equals(desk)) {
                equitiesTotal = total;
                equitiesBreach = breach;
            } else if ("CREDIT".equals(desk)) {
                creditTotal = total;
                creditBreach = breach;
            }
        }

        // Desk EQUITIES
        assertNotNull(equitiesTotal, "EQUITIES should be in client output");
        assertEquals(120.0, equitiesTotal, 0.0001, "Unexpected EQUITIES exposure");
        assertNotNull(equitiesBreach, "EQUITIES breach flag should be present");
        assertTrue(equitiesBreach, "EQUITIES should breach limit 100.0");

        // Desk CREDIT
        assertNotNull(creditTotal, "CREDIT should be in client output");
        assertEquals(50.0, creditTotal, 0.0001, "Unexpected CREDIT exposure");
        assertNotNull(creditBreach, "CREDIT breach flag should be present");
        assertFalse(creditBreach, "CREDIT should NOT breach limit 100.0");
    }

    private String getData(){
        return "trade_id,desk,notional,currency\n" +
                "T1,EQUITIES,100.0,EUR\n" +
                "T2,EQUITIES,20.0,EUR\n" +
                "T3,CREDIT,50.0,USD\n";
    }
}