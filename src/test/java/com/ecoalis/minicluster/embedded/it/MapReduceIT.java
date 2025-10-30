package com.ecoalis.minicluster.embedded.it;

import com.ecoalis.minicluster.core.ClusterRuntimeConfig;
import com.ecoalis.minicluster.job.WordCountMapReduceJob;
import com.ecoalis.minicluster.modules.mapreduce.MapReduceServiceHandle;
import com.ecoalis.minicluster.embedded.testsupport.AbstractIntegrationTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test d'intégration MapReduce "local mode" (sans YARN).
 * <p>
 * * On démarre HDFS + MapReduce local
 * * On écrit un input dans HDFS
 * * On lance un job WordCount
 * * On vérifie la sortie
 * <p>
 * Compatible Windows, pas besoin de MiniYARNCluster.
 */
public class MapReduceIT extends AbstractIntegrationTest {

    @Override
    protected ClusterRuntimeConfig runtimeConfig() {
        return ClusterRuntimeConfig.builder()
                .withMapReduce() // <- pas besoin de withYarn()
                .build();
    }

    @Test
    public void should_run_wordcount_job_locally_against_hdfs() throws Exception {
        // Given : Préparation d'un input dans HDFS
        writeStringToHdfs("/mr/input/data.txt", "hello hello world");

        // When : Récupération de la conf MR locale fournie par le runtime
        assertTrue(runtime.getMapReduceHandle().isPresent(), "MapReduce should be enabled");
        MapReduceServiceHandle mrHandle = runtime.getMapReduceHandle().get();
        Configuration mrConf = mrHandle.getConfiguration();

        String inputHdfs  = runtime.getHdfsUri() + "/mr/input";
        String outputHdfs = runtime.getHdfsUri() + "/mr/output";

        boolean success = WordCountMapReduceJob.run(mrConf, inputHdfs, outputHdfs);
        assertTrue(success, "MapReduce job should complete successfully");

        // Then : Lire la sortie (part-r-00000)
        FileSystem fs = runtime.getFileSystem();
        Path outputDir = new Path("/mr/output");

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(outputDir, false);
        Map<String,Integer> counts = new HashMap<String,Integer>();

        while (files.hasNext()) {
            LocatedFileStatus st = files.next();
            String name = st.getPath().getName();
            if (!name.startsWith("part-")) {
                continue; // skip _SUCCESS
            }

            try (FSDataInputStream in = fs.open(st.getPath());
                 BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

                String line;
                while ((line = br.readLine()) != null) {
                    String[] split = line.split("\\t");
                    if (split.length == 2) {
                        counts.put(split[0], Integer.parseInt(split[1]));
                    }
                }
            }
        }

        // Assertions métier
        assertEquals(Integer.valueOf(2), counts.get("hello"), "Expected hello=2");
        assertEquals(Integer.valueOf(1), counts.get("world"), "Expected world=1");
    }
}