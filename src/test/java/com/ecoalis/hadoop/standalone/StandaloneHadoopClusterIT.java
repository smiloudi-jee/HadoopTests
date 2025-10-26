package com.ecoalis.hadoop.standalone;

import com.ecoalis.hadoop.standalone.client.StandaloneClusterClient;
import org.junit.jupiter.api.*;

import java.io.*;
import java.sql.*;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests JUnit 5 (un par mode d’accès + health)
 * * *************************************************************
 * Integration tests for a standalone Hadoop cluster:
 * - Health check
 * - WebHDFS basic operations
 * - Hive JDBC query
 * - Spark SQL via REST API
 * - Large file upload/download via WebHDFS chunked transfer
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StandaloneHadoopClusterIT {

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

    @Test @Order(1)
    void health_shouldBeUp() throws Exception {
        String json = client.health();
        assertTrue(json.contains("\"hdfs\":\"UP\""));
        assertTrue(json.contains("\"spark\":\"UP\""));
        assertTrue(json.contains("\"hiveServer2\":\"UP\""));
    }

    @Test @Order(2)
    void webhdfs_ls_shouldListRoot() throws Exception {
        String json = client.hdfsLs("/");
        assertTrue(json.contains("FileStatuses"));
    }

    @Test @Order(3)
    void jdbc_shouldExecuteSimpleQuery() throws Exception {
        try (Connection cnx = client.openHive();
             Statement st = cnx.createStatement();
             ResultSet rs = st.executeQuery("SELECT 1+1 AS two")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test @Order(4)
    void restsql_shouldCreateAndQuery() throws Exception {
        client.restSql("CREATE DATABASE IF NOT EXISTS demo");
        client.restSql("USE demo");
        client.restSql("CREATE OR REPLACE TEMP VIEW v AS SELECT 42 AS x");
        String r = client.restSql("SELECT * FROM v");
        assertTrue(r.contains("\"data\""));
    }

    // ---- Export/Import volumineux via WebHDFS ----
    @Test @Order(5)
    void webhdfs_chunked_upload_download_shouldMatch() throws Exception {
        // Fichier ~5 MiB de données pseudo-aléatoires
        byte[] inMem = new byte[5 * 1024 * 1024];
        new Random(1234).nextBytes(inMem);
        ByteArrayInputStream bais = new ByteArrayInputStream(inMem);

        String dest = "/starter/bigfile.bin";
        client.hdfsPutChunked(dest, bais, 64); // chunk 64 KB

        ByteArrayOutputStream baos = new ByteArrayOutputStream(inMem.length);
        client.hdfsDownload(dest, baos);
        byte[] outMem = baos.toByteArray();

        assertEquals(inMem.length, outMem.length);
        assertArrayEquals(inMem, outMem);
    }}
