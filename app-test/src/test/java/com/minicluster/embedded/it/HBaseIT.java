package com.minicluster.embedded.it;

import com.minicluster.core.ClusterRuntimeConfig;
import com.minicluster.modules.hbase.HBaseServiceHandle;
import com.minicluster.testsupport.AbstractIntegrationTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test d'intégration HBase embarqué :
 * * démarre un mini-cluster HBase
 * * crée une table
 * * écrit une ligne
 * * lit la ligne
 */
public class HBaseIT extends AbstractIntegrationTest {

    @Override
    protected ClusterRuntimeConfig runtimeConfig() {
        return ClusterRuntimeConfig.builder()
                .withHBase()
                .build();
    }

    @Test
    public void hbase_should_create_and_get_row() throws Exception {
        assertTrue(runtime.getHBaseHandle().isPresent(), "HBase should be enabled");
        HBaseServiceHandle hbase = runtime.getHBaseHandle().get();

        Connection conn = hbase.getConnection();
        assertNotNull(conn, "HBase connection should not be null");

        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("test_table");
        byte[] cfBytes = "cf".getBytes(StandardCharsets.UTF_8);

        // 1. Créer la table si elle n'existe pas encore
        if (!admin.tableExists(tableName)) {
            TableDescriptorBuilder tableDescBuilder =
                    TableDescriptorBuilder.newBuilder(tableName);

            ColumnFamilyDescriptor cfDesc =
                    ColumnFamilyDescriptorBuilder
                            .newBuilder(cfBytes)
                            .build();

            tableDescBuilder.setColumnFamily(cfDesc);

            admin.createTable(tableDescBuilder.build());
        }

        // 2. Écrire une ligne
        Table table = conn.getTable(tableName);
        byte[] rowKey = "row1".getBytes(StandardCharsets.UTF_8);

        Put put = new Put(rowKey);
        put.addColumn(cfBytes,
                "field".getBytes(StandardCharsets.UTF_8),
                "hello-hbase".getBytes(StandardCharsets.UTF_8));
        table.put(put);

        // 3. Lire la même ligne
        Get get = new Get(rowKey);
        Result result = table.get(get);

        byte[] value = result.getValue(cfBytes, "field".getBytes(StandardCharsets.UTF_8));
        assertNotNull(value, "Expected value in HBase row");
        assertEquals("hello-hbase", new String(value, StandardCharsets.UTF_8));

        table.close();
        admin.close();
    }
}
