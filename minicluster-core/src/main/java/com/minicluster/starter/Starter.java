package com.minicluster.starter;

import com.minicluster.core.ClusterRuntime;
import com.minicluster.core.ClusterRuntimeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.minicluster.util.HadoopConstants.REST_ENABLED;

/**
 * Petit exécutable manuel pour lancer le mini-cluster en mode "full stack".
 */
public final class Starter {
    private static final Logger LOGGER = LoggerFactory.getLogger(Starter.class);
    private static ClusterRuntime runtime;

    public static void main(String[] args) throws Exception {
        // Charger mini-cluster.properties du classpath
        Properties props = loadProps();
        props.setProperty(REST_ENABLED, "true");

        ClusterRuntimeConfig cfg = ClusterRuntimeConfig.builder()
                .withHive()
                .withSpark()
                .withRest()
                .withThrift()
                .props(props)
                .advertisedHost(props.getProperty("advertisedhost", null))
                .build();

        try {
            runtime = ClusterRuntime.start(cfg);
            LOGGER.info("MiniCluster running.");
            LOGGER.info("HDFS URI : {}", runtime.getHdfsUri());
            runtime.getRestHandle().ifPresent(rest ->
                    LOGGER.info("REST port: {}", rest.getPort())
            );
        } catch (Exception e) {
            shutdownQuiet();
            throw e;
        }
        // Arrêt propre sur Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(Starter::shutdownQuiet));
        LOGGER.info("MiniCluster en cours d’exécution. Presse Ctrl+C pour arrêter.");
        new java.util.concurrent.CountDownLatch(1).await();
    }

    private static Properties loadProps() throws IOException {
        Properties props = new Properties();
        try (InputStream in = Starter.class.getResourceAsStream("/mini-cluster.properties")) {
            if (in == null) {
                LOGGER.error("mini-cluster.properties non trouvé, utilisation valeurs par défaut.");
            } else {
                props.load(in);
            }
        }
        return props;
    }

    private static void shutdownQuiet() {
        LOGGER.info("\nArrêt du MiniDFS…");
        try {
            if (runtime != null) runtime.close();
        } catch (Exception ignore) {
        }
        LOGGER.info("MiniCluster arrêté proprement.");
    }

}