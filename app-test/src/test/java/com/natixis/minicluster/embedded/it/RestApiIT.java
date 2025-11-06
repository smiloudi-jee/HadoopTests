package com.natixis.minicluster.embedded.it;

import com.natixis.minicluster.client.StandaloneClusterClient;
import com.natixis.minicluster.core.ClusterRuntimeConfig;
import com.natixis.minicluster.modules.rest.response.HdfsEntry;
import com.natixis.minicluster.modules.rest.response.RestHealthResponse;
import com.natixis.minicluster.modules.rest.RestServiceHandle;
import com.natixis.minicluster.testsupport.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test d'intégration de l'API REST exposée par le mini-cluster.
 * Ici, on passe par StandaloneClusterClient (client applicatif)
 * plutôt que d'appeler directement via HttpClient.
 */
public class RestApiIT extends AbstractIntegrationTest {

    @Override
    protected ClusterRuntimeConfig runtimeConfig() {
        java.util.Properties p = new java.util.Properties();
        p.setProperty("rest.enabled", "true"); // On veut REST + Spark (car /sql/execute dépend du moteur Spark/Hive)
        p.setProperty("spark.enabled", "true"); // On veut Hive aussi, car Spark est lancé avec enableHiveSupport().
        p.setProperty("sparkHive.enabled", "true");

        return ClusterRuntimeConfig.builder()
                .withHive()
                .withSpark()
                .withRest()
                .props(p)
                .build();
    }

    /**
     * Teste que l'API REST expose bien les endpoints /health, /hdfs/ls et /sql/execute.
     */
    @Test
    void rest_api_should_expose_health_hdfs_and_sql_via_client() throws Exception {
        // 1. On récupère le handle REST lancé par le runtime
        assertTrue(runtime.getRestHandle().isPresent(), "REST should be enabled for this runtime");
        RestServiceHandle restHandle = runtime.getRestHandle().get();
        int restPort = restHandle.getPort();
        assertTrue(restPort > 0, "REST port should be > 0");

        // 2. On instancie le client applicatif en lui donnant l'URL base
        //    On suppose http://localhost:<port> comme base URL (à adapter si ton client attend autre chose).
        StandaloneClusterClient client = new StandaloneClusterClient("http://localhost:" + restPort);

        // 3. /health
        RestHealthResponse health = client.getHealth();
        assertNotNull(health, "health response should not be null");
        assertEquals("200", health.getStatus(), "cluster should be UP via /health");
        assertNotNull(health.getHdfs(), "health.hdfsUri should not be null");
        // si ton objet health expose aussi spark info, tu peux l'asserter ici :
        // assertNotNull(health.sparkVersion(), "sparkVersion should be present");

        // 4. Préparons des données dans HDFS via le runtime (pas via REST),
        //    puis vérifions que le client peut les voir via l'endpoint /hdfs/ls.
        writeStringToHdfs("/tmp/from-test-api.txt", "bonjour rest");

        List<HdfsEntry> hdfsListing = client.listHdfs("/tmp");
        boolean found = false;
        for (HdfsEntry e : hdfsListing) {
            if (e.getPath() != null && e.getPath().contains("from-test-api.txt")) {
                found = true;
                break;
            }
        }
        assertTrue(found,
                "HDFS listing from REST should contain the file 'from-test-api.txt', got: " + hdfsListing);


        // 5. /sql/execute
        //    On suppose que ton client expose une méthode executeSql(String sql)
        //    qui renvoie soit une structure résultat, soit juste un String.
        String sqlResult = client.executeSql("SELECT to_upper('france') AS val");
        assertNotNull(sqlResult, "SQL result should not be null");
        assertTrue(
                sqlResult.toUpperCase().contains("FRANCE"),
                "SQL result should contain FRANCE from to_upper udf, got: " + sqlResult
        );
    }
}