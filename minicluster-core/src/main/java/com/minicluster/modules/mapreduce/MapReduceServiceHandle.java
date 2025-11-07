package com.minicluster.modules.mapreduce;

import org.apache.hadoop.conf.Configuration;

/**
 * Handle MapReduce pour tests.
 * * En mode "local", pas de cluster yarn extérieur,
 * * fournit simplement une Configuration MR prête à l'emploi.
 */
public class MapReduceServiceHandle implements AutoCloseable {

    private final Configuration mrConf;

    public MapReduceServiceHandle(Configuration mrConf) {
        this.mrConf = mrConf;
    }

    /**
     * Conf MR à utiliser pour lancer les jobs MapReduce.
     */
    public Configuration getConfiguration() {
        return mrConf;
    }

    @Override
    public void close() {
        // Rien à arrêter en mode local
        // Cluster HDFS externe géré par ClusterRuntime
        // La méthode est là pour respecter l'interface AutoCloseable
    }
}