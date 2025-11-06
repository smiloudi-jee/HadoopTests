package com.natixis.minicluster.testsupport;

import com.natixis.minicluster.core.ClusterRuntime;
import com.natixis.minicluster.core.ClusterRuntimeConfig;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIntegrationTest {

    protected ClusterRuntime runtime;

    @BeforeAll
    void beforeAll() throws Exception {
        runtime = ClusterRuntime.start(runtimeConfig());
    }

    @AfterAll
    void afterAll() {
        if (runtime != null) {
            runtime.close();
        }
    }

    /**
     * Chaque test concret doit définir quels services il veut :
     * - only HDFS     => builder() sans withSpark/withHive/withRest
     * - Spark/Hive    => withSpark().withHive()
     * - REST aussi    => withRest()
     * - Thrift serveur JDBC => withThrift()
     */
    protected abstract ClusterRuntimeConfig runtimeConfig();

    /**
     * Helper rapide : écrire un string dans HDFS.
     * Tu peux aussi décider de déléguer à IntegrationTestSupport.writeStringToHdfs(...)
     */
    protected void writeStringToHdfs(String hdfsPath, String content) throws Exception {
        FileSystem fs = runtime.getFileSystem();
        Path p = new Path(hdfsPath);

        if (!fs.exists(p.getParent())) {
            fs.mkdirs(p.getParent());
        }
        try (FSDataOutputStream out = fs.create(p, true)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Accès Spark depuis un test qui a activé withSpark() dans runtimeConfig().
     */
    protected SparkSession spark() {
        return runtime.getSparkSession()
                .orElseThrow(() ->
                        new IllegalStateException("Spark not enabled for this test runtime"));
    }


    /**
     * Ecrit un contenu texte dans HDFS à l'emplacement donné.
     * Variante statique si tu veux éviter d'appeler AbstractIntegrationTest.writeStringToHdfs.
     */
    public static void writeStringToHdfs(FileSystem fs, String hdfsPath, String content) throws Exception {
        Path p = new Path(hdfsPath);
        if (!fs.exists(p.getParent())) {
            fs.mkdirs(p.getParent());
        }
        try (FSDataOutputStream out = fs.create(p, true)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Exécute une requête SQL Spark/Hive et renvoie le DataFrame résultat.
     */
    public static Dataset<Row> runSql(SparkSession spark, String sql) {
        return spark.sql(sql);
    }

    /**
     * Récupère la première colonne du DataFrame en tant que liste de Strings.
     * Pratique pour vérifier le résultat de UDFs ou de petits SELECTs.
     */
    public static List<String> collectSingleColumn(Dataset<Row> df) {
        return df.collectAsList()
                .stream()
                .map(row -> row.get(0) == null ? null : row.get(0).toString())
                .collect(Collectors.toList());
    }

    /**
     * Version nommée de collectSingleColumn quand tu veux cibler une colonne précise
     * (ex: SELECT col1, col2 ...).
     */
    public static List<String> collectColumn(Dataset<Row> df, String colName) {
        return df.select(colName)
                .collectAsList()
                .stream()
                .map(row -> row.get(0) == null ? null : row.get(0).toString())
                .collect(Collectors.toList());
    }

    /**
     * Assertion utilitaire : vérifie qu'une valeur attendue est bien présente dans la liste.
     */
    public static void assertContains(List<String> values, String expected) {
        assertTrue(
                values.contains(expected),
                "Expected to find [" + expected + "] in " + values
        );
    }

}