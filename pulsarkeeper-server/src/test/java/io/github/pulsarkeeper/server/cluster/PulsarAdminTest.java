package io.github.pulsarkeeper.server.cluster;

import io.github.pulsarkeeper.server.base.MockedPulsarServiceBaseTest;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PulsarAdminTest extends MockedPulsarServiceBaseTest {

    private PulsarAdmin admin;
    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setMessagingProtocols(Set.of("pulsarkeeper"));
        String path = this.getClass().getClassLoader().getResource("ignore.txt").getPath()
                .replace("resources/test/ignore.txt", "libs");
        this.conf.setProtocolHandlerDirectory(path);
    }

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        internalSetup();
        setupDefaultTenantAndNamespace();
        admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://127.0.0.1:9023")
                .build();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void list() throws PulsarAdminException {
        List<String> clusters = admin.clusters().getClusters();
        Assert.assertEquals(clusters.size(), 1);
        Assert.assertEquals(clusters.stream().findFirst().get(), CONFIG_CLUSTER_NAME);
    }

    @Test(priority = 1)
    public void curdSuccess() {
        final String clusterName = "example";
        ClusterDataImpl clusterData = ClusterDataImpl.builder()
                .serviceUrl("pulsar://127.0.0.1:6650")
                .build();
        admin.clusters().createClusterAsync(clusterName, clusterData).join();
        ClusterData getClusterData = admin.clusters().getClusterAsync(clusterName).join();
        Assert.assertEquals(getClusterData, clusterData);
        admin.clusters().deleteClusterAsync(clusterName).join();
        List<String> clusters = admin.clusters().getClustersAsync().join();
        Assert.assertFalse(clusters.contains(clusterName));
    }
}