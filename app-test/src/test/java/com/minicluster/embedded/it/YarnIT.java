package com.minicluster.embedded.it;

import com.minicluster.core.ClusterRuntime;
import com.minicluster.core.ClusterRuntimeConfig;
import com.minicluster.modules.yarn.YarnServiceHandle;
import com.minicluster.testsupport.AbstractIntegrationTest;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.minicluster.util.HadoopConstants.IS_WINDOWS;
import static org.junit.jupiter.api.Assertions.*;

public class YarnIT extends AbstractIntegrationTest {

    @BeforeAll
    void beforeAll() throws Exception {
        Assumptions.assumeFalse(IS_WINDOWS,
                "MiniYARNCluster désactivé sous Windows (problème chemins/symlinks).");

        runtime = ClusterRuntime.start(runtimeConfig());
    }

    @Override
    protected ClusterRuntimeConfig runtimeConfig() {
        java.util.Properties p = new java.util.Properties();
        // Pour l’instant pas besoin de Spark/Hive/REST, juste YARN + HDFS
        return ClusterRuntimeConfig.builder()
                .withYarn()
                .props(p)
                .build();
    }

    @Test
    public void yarn_cluster_should_start() {
        assertTrue(runtime.getYarnHandle().isPresent(), "YARN should be enabled");
        YarnServiceHandle handle = runtime.getYarnHandle().get();

        assertNotNull(handle.getYarnCluster(), "MiniYARNCluster should not be null");

        // ResourceManager doit être vivant
        assertNotNull(handle.getYarnCluster().getResourceManager(),
                "ResourceManager should be up");
        assertFalse(handle.getYarnCluster().getResourceManager().getServiceState().toString().contains("STOPPED"),
                "ResourceManager should not be STOPPED");

        // Au moins un NodeManager a démarré
        assertNotNull(handle.getYarnCluster().getNodeManager(0),
                "At least one NodeManager should be available");
    }
}