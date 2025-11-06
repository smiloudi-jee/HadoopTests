package com.natixis.minicluster.modules.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.Properties;

import static com.natixis.minicluster.util.HadoopConstants.DEFAULT_BASE_DIR;

/**
 * Démarre un PigServer en mode MAPREDUCE local, branché sur l'HDFS du mini-cluster.
 */
public final class PigBootstrap {

    private PigBootstrap() {}

    /**
     * @param hadoopConf  Conf Hadoop du runtime (celle de ton MiniDFSCluster)
     * @param hdfsUri     URI HDFS du runtime (ex: hdfs://localhost:20112)
     */
    public static PigServiceHandle startPig(Configuration hadoopConf, String hdfsUri) throws Exception {
        // 1) Dossier conf temporaire ajouté au classpath
        File confDir = new File(DEFAULT_BASE_DIR); // ou un sous-dossier de ton workdir
        if (!confDir.exists()) {
            confDir.mkdirs();
        }

        // 2) Ecrire un core-site.xml minimal (depuis ta conf)
        //    NB: on s'assure que fs.defaultFS & MR local y sont bien présents
        Configuration core = new Configuration(false); // vide, on met juste le nécessaire
        core.set("fs.defaultFS", hdfsUri);                  // pointe vers TON HDFS
        core.set("mapreduce.framework.name", "local");      // MR local
        core.set("mapred.job.tracker", "local");            // compat
        core.set("io.file.buffer.size", "131072");          // confort
        // (ajoute ici d'autres clés utiles s'il en faut)

        File coreSite = new File(confDir, "core-site.xml");
        try (FileOutputStream fos = new FileOutputStream(coreSite)) {
            core.writeXml(fos);
        }

        // 3) Injecter confDir dans le classpath via le TCCL
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = new URLClassLoader(
                new URL[]{ confDir.toURI().toURL() },
                prev
        );
        Thread.currentThread().setContextClassLoader(cl);

        // 4) Propriétés Pig
        Properties props = new Properties();
        // Important: Pig respectera le core-site.xml qu’il voit au classpath,
        // mais on double avec ces props (no harm):
        props.setProperty("fs.defaultFS", hdfsUri);
        props.setProperty("mapreduce.framework.name", "local");
        props.setProperty("mapred.job.tracker", "local");
        props.setProperty("pig.use.overridden.hadoop.configs", "true");
        props.setProperty("pig.exec.reducers.max", "1");

        PigServer pig = null;
        boolean ok = false;
        try {
            pig = new PigServer(ExecType.MAPREDUCE, props);
            pig.setBatchOn();
            ok = true;
            // On rend le handle qui saura fermer Pig; on lui passe aussi de quoi restaurer le CL
            return new PigServiceHandle(pig, hdfsUri, cl, prev);
        } finally {
            if (!ok) {
                // si l'init Pig plante, on restaure le CL tout de suite
                Thread.currentThread().setContextClassLoader(prev);
                try { if (pig != null) pig.shutdown(); } catch (Exception ignore) {}
                // nettoyage optionnel
                try { Files.deleteIfExists(coreSite.toPath()); } catch (Exception ignore) {}
            }
        }
    }

    private static Configuration buildPigConfiguration(Configuration hadoopConf, String hdfsUri){
        Configuration configuration  = new Configuration(hadoopConf);

        // branché à l'HDFS du mini-cluster
        configuration.set("fs.defaultFS", hdfsUri);

        // MR local -> pas de YARN; submit local runner
        configuration.set("mapreduce.framework.name", "local");
        configuration.set("mapred.job.tracker", "local"); // compat

        // Important pour que Pig respecte nos overrides Hadoop
        configuration.set("pig.use.overridden.hadoop.configs", "true");

        // Quelques garde-fous utiles
        configuration.set("pig.exec.reducers.max", "1"); // simple et déterministe
        configuration.set("io.file.buffer.size", "131072");

        return configuration;
    }
}