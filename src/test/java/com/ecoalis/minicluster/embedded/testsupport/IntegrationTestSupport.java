package com.ecoalis.minicluster.embedded.testsupport;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public final class IntegrationTestSupport {

    private IntegrationTestSupport() {}

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