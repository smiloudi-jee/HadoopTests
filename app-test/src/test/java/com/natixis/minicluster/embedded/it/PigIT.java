package com.natixis.minicluster.embedded.it;

import com.natixis.minicluster.core.ClusterRuntimeConfig;
import com.natixis.minicluster.testsupport.AbstractIntegrationTest;
import com.natixis.minicluster.core.ClusterRuntime;
import com.natixis.minicluster.modules.pig.PigBootstrap;
import com.natixis.minicluster.modules.pig.PigServiceHandle;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class PigIT extends AbstractIntegrationTest {

    @Override
    protected ClusterRuntimeConfig runtimeConfig() {
        return ClusterRuntimeConfig.builder()
                .withPig()
                .build();
    }

    @Test
    public void should_run_pig_mapreduce_local_against_hdfs() throws Exception {
        // Given : Préparation d'un input & output dans HDFS
        String inputPath = "/pig/input/data.csv";
        String outputDir = "/pig/output";
        writeStringToHdfs(inputPath, "A,10\n" + "A,20\n" + "B,5\n" + "A,7\n" + "B,3\n");
        FileSystem fs = runtime.getFileSystem();
        // Démarrer Pig (MR local branché sur HDFS)
        PigServiceHandle pig = PigBootstrap.startPig(fs.getConf(), runtime.getHdfsUri());

        // When : Script Pig : Count par clé (col0)
        assertTrue(runtime.getPigHandle().isPresent(), "Pig not enabled");
        pig.runScript(scriptPig(runtime, inputPath, outputDir));
        pig.close();

        // Then : Lire la sortie sur HDFS et valider
        Path out = new Path(outputDir);
        assertTrue(fs.exists(out), "Output dir should exist");

        Map<String, Integer> counts = new HashMap<>();
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(out, false);
        while (it.hasNext()) {
            LocatedFileStatus st = it.next();
            String name = st.getPath().getName();
            if (!name.startsWith("part")) continue;

            try (FSDataInputStream in = fs.open(st.getPath());
                 BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] p = line.split("\\t");
                    if (p.length == 2) {
                        counts.put(p[0], Integer.parseInt(p[1]));
                    }
                }
            }
        }
        // Données: A,A,B,A,B  -> A=3, B=2
        assertEquals(Integer.valueOf(3), counts.get("A"), "A count");
        assertEquals(Integer.valueOf(2), counts.get("B"), "B count");

    }

    /**
     * Script Pig : Count par clé (col0)
     * * LOAD depuis HDFS (TextLoader, puis SPLIT/FOREACH pour parser)
     * * GROUP par clé
     * * GENERATE COUNT
     * * STORE sur HDFS
     *
     * @param runtime current ClusterRuntime
     * @param inputPath input path on HDFS, where are input entries
     * @param outputDir outputDIR path on HDFS, where get the result of script
     * @return script
     *
     */
    private String scriptPig(ClusterRuntime runtime, String inputPath, String outputDir) {
        return "raw = LOAD '" + runtime.getHdfsUri() + inputPath +
                "' USING org.apache.pig.piggybank.storage.CSVLoader() AS (k:chararray, v:int);\n" +
                "grp = GROUP raw BY k;\n" + "agg = FOREACH grp GENERATE group AS k, COUNT(raw) AS cnt;\n" +
                "STORE agg INTO '" + runtime.getHdfsUri() + outputDir + "' USING org.apache.pig.builtin.PigStorage('\\t');";
    }

    /**
     * Script Pig : Count par clé (col0) / independent de PiggyBank, alternative manuel parsing:
     * * LOAD depuis HDFS (TextLoader, puis SPLIT/FOREACH pour parser)
     * * GROUP par clé
     * * GENERATE COUNT
     * * STORE sur HDFS
     *
     * @param runtime current ClusterRuntime
     * @param inputPath input path on HDFS, where are input entries
     * @param outputDir outputDIR path on HDFS, where get the result of script
     * @return script
     *
     */
    private String scriptPigAlternate(ClusterRuntime runtime, String inputPath, String outputDir) {
        return  "lines = LOAD '" + runtime.getHdfsUri() + inputPath + "' USING TextLoader() AS (line:chararray);\n" +
                "fields = FOREACH lines GENERATE " +
                "FLATTEN(STRSPLIT(line, ',', 2)) AS (k:chararray, v:chararray);\n" +
                "grp = GROUP fields BY k;\n" +
                "agg = FOREACH grp GENERATE group AS k, COUNT(fields) AS cnt;\n" +
                "STORE agg INTO '" + runtime.getHdfsUri() + outputDir + "' USING PigStorage('\\t');";
    }
}
