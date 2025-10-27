package com.ecoalis.minicluster.starter;

import com.ecoalis.minicluster.core.ClusterRuntime;
import com.ecoalis.minicluster.core.ClusterRuntimeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Petit exécutable manuel pour lancer le mini-cluster en mode "full stack".
 */
public final class Starter {
    private static final Logger LOGGER = LoggerFactory.getLogger(Starter.class);
    private static ClusterRuntime runtime;

    public static void main(String[] args) throws Exception {
        // Charger mini-hdfs.properties du classpath
        Properties props = loadProps();

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
            System.out.println("MiniCluster running.");
            System.out.println("HDFS URI : " + runtime.getHdfsUri());
            runtime.getRestHandle().ifPresent(rest ->
                    System.out.println("REST port: " + rest.getPort())
            );
        } catch (Exception e) {
            shutdownQuiet();
            throw e;
        }
        // Arrêt propre sur Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(Starter::shutdownQuiet));
        LOGGER.info("MiniDFS en cours d’exécution. Presse Ctrl+C pour arrêter.");
        new java.util.concurrent.CountDownLatch(1).await();
    }

    private static Properties loadProps() throws IOException {
        Properties props = new Properties();
        try (InputStream in = Starter.class.getResourceAsStream("/mini-hdfs.properties")) {
            if (in == null) {
                System.err.println("mini-hdfs.properties non trouvé, utilisation valeurs par défaut.");
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
        LOGGER.info("MiniDFS arrêté proprement.");
    }

}