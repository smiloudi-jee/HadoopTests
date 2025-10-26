package com.ecoalis.hadoop.standalone;

import com.ecoalis.hadoop.standalone.client.StandaloneClusterClient;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for various data formats (Parquet, ORC, CSV) using Hive JDBC and REST API.
 * * Formats de données (Parquet / ORC via JDBC, CSV via REST) + CTAS & INSERT OVERWRITE
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DataFormatsIT {

    static StandaloneClusterClient client;

    @BeforeAll
    static void init() {
        String host      = System.getProperty("host", "localhost");
        String nnHttp    = System.getProperty("nn.http.port", "9870");
        String thrift    = System.getProperty("thrift.port", "10000");
        String restPort  = System.getProperty("rest.port", "18080");

        String webhdfs   = "http://" + host + ":" + nnHttp + "/webhdfs/v1";
        String hiveUrl   = "jdbc:hive2://" + host + ":" + thrift + "/default;transportMode=binary";
        String restBase  = "http://" + host + ":" + restPort;

        client = new StandaloneClusterClient(webhdfs, hiveUrl, restBase);
    }

    /**
     * Parquet CTAS + "overwrite logique" via staging table (compatible Spark ThriftServer)
     */
    @Test
    @Order(1)
    void parquet_ctas_and_insert_overwrite_via_jdbc() throws Exception {
        try (Connection c = client.openHive();
             Statement st = c.createStatement()) {

            // 1. Sélectionne / crée la base & Nettoyage préalable
            st.execute("CREATE DATABASE IF NOT EXISTS demo");
            st.execute("USE demo");
            st.execute("DROP TABLE IF EXISTS t_parquet");
            st.execute("DROP TABLE IF EXISTS t_parquet_staging");

            // 2. CTAS Parquet : on crée la table principale t_parquet
            st.execute("CREATE TABLE t_parquet STORED AS PARQUET AS " +
                "SELECT stack(3, 1,'Alice', 2,'Bob', 3,'Charlie') AS (id, name)");

            // 3. Sanity check : t_parquet doit contenir 3 lignes
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t_parquet");
            assertTrue(rs.next(), "COUNT(*) should return one row");
            assertEquals(3L, rs.getLong(1), "t_parquet should have 3 rows after CTAS");

            // 4. Crée une table staging avec les noms uppercased
            st.execute("CREATE TABLE t_parquet_staging STORED AS PARQUET AS " +
                    "SELECT id, UPPER(name) AS name FROM t_parquet");

            // 5. Vérif rapide staging
            rs = st.executeQuery("SELECT COUNT(*) FROM t_parquet_staging");
            assertTrue(rs.next(), "COUNT(*) from staging should return one row");
            assertEquals(3L, rs.getLong(1), "staging should also have 3 rows");

            // 6. On vide la table d'origine : TRUNCATE TABLE marche en mode Hive catalog avec tables gérées parquet
            st.execute("TRUNCATE TABLE t_parquet");

            // 7. On recharge depuis la staging : (INSERT INTO sans OVERWRITE évite le check Catalyst qui te bloque)
            st.execute("INSERT INTO t_parquet SELECT id, name FROM t_parquet_staging");

            // 8. Drop de la staging (propreté)
            st.execute("DROP TABLE t_parquet_staging");

            // 9. Vérifs finales : tout doit être uppercased maintenant
            rs = st.executeQuery("SELECT MIN(LENGTH(name)) AS min_len, MAX(LENGTH(name)) AS max_len, " +
                "SUM(CASE WHEN name = UPPER(name) THEN 1 ELSE 0 END) AS upper_cnt, COUNT(*) AS total_cnt FROM t_parquet");

            assertTrue(rs.next(), "Validation query should return one row");

            int minLen = rs.getInt("min_len");
            int maxLen = rs.getInt("max_len");
            int upperCount = rs.getInt("upper_cnt");
            int totalCount = rs.getInt("total_cnt");

            // Sanity checks
            assertEquals(3, totalCount, "We should still have 3 rows after reload");
            assertTrue(minLen >= 3, "All names should still have length >= 3");
            assertTrue(maxLen <= 7, "No unexpected explosion in name length");
            assertEquals(totalCount, upperCount, "All names should now be UPPER()d");
        }
    }


    /**
     * ORC partitioned table + INSERT OVERWRITE PARTITION via JDBC
     */
    @Test @Order(2)
    void orc_partitioned_table_via_jdbc() throws Exception {
        try (Connection c = client.openHive(); Statement st = c.createStatement()) {
            st.execute("USE demo");
            st.execute("DROP TABLE IF EXISTS t_orc_part");
            st.execute("CREATE TABLE t_orc_part (id INT, name STRING) PARTITIONED BY (p_date STRING) STORED AS ORC");

            // Ajouter des partitions (INSERT OVERWRITE PARTITION)
            st.execute("INSERT OVERWRITE TABLE t_orc_part PARTITION (p_date='2025-10-01') SELECT 1,'Alice'");
            st.execute("INSERT OVERWRITE TABLE t_orc_part PARTITION (p_date='2025-10-02') SELECT 2,'Bob'");
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t_orc_part WHERE p_date >= '2025-10-01'");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    /**
     * CSV external table via REST API
     */
    @Test @Order(3)
    void csv_external_via_rest_sql() throws Exception {
        // Spark SQL côté serveur (via REST) -> USING CSV
        client.restSql("USE demo");
        client.restSql("DROP TABLE IF EXISTS t_csv");
        client.restSql(
                "CREATE TABLE t_csv USING csv OPTIONS (path 'hdfs:///starter/csv/t_csv', header 'true') " +
                        "AS SELECT 10 as id, 'John' as name UNION ALL SELECT 20, 'Jane'"
        );
        String res = client.restSql("SELECT COUNT(*) AS c FROM t_csv");
        assertTrue(res.contains("\"data\""));
        assertTrue(res.contains("2")); // très simple, on vérifie qu'on voit '2' dans la sortie JSON
    }
}
