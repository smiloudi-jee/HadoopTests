package com.ecoalis.hadoop.standalone;

import com.ecoalis.hadoop.standalone.client.StandaloneClusterClient;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for user-defined functions (User Defined Functions) in Hive using JDBC and REST API.
 * * Tests des fonctions définies par l'utilisateur (User Defined Function) via JDBC et REST
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class UdfIT {

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

    @Test
    void udf_to_upper_shouldWork_via_jdbc() throws Exception {
        try (Connection c = client.openHive(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT to_upper('alice')");
            assertTrue(rs.next());
            assertEquals("ALICE", rs.getString(1));
        }
    }

    // Test UDF via REST API
    @Test
    void udf_to_upper_shouldWork_via_rest() throws Exception {
        String out = client.restSql("SELECT to_upper('bob') AS b");
        assertTrue(out.toUpperCase().contains("BOB"));
    }
}
