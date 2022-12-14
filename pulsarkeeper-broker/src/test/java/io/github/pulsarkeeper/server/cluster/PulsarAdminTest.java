package io.github.pulsarkeeper.server.cluster;

import io.github.pulsarkeeper.server.base.MockedPulsarServiceBaseTest;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PulsarAdminTest extends MockedPulsarServiceBaseTest {

    private PulsarAdmin admin;

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        internalSetup();
        setupDefaultTenantAndNamespace();
        admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://127.0.0.1:" + pulsarKeeperPort)
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
                .brokerServiceUrl("pulsar://127.0.0.1:6650")
                .build();
        admin.clusters().createClusterAsync(clusterName, clusterData).join();
        ClusterData getClusterData = admin.clusters().getClusterAsync(clusterName).join();
        Assert.assertEquals(getClusterData, clusterData);
        ClusterDataImpl newClusterData = ClusterDataImpl.builder()
                .brokerServiceUrl("pulsar://127.0.0.1:6651")
                .build();
        admin.clusters().updateClusterAsync(clusterName, newClusterData).join();
        ClusterData getNewClusterData = admin.clusters().getClusterAsync(clusterName).join();
        Assert.assertEquals(getNewClusterData, newClusterData);
        admin.clusters().deleteClusterAsync(clusterName).join();
        List<String> clusters = admin.clusters().getClustersAsync().join();
        Assert.assertFalse(clusters.contains(clusterName));
    }

    @Test(priority = 1)
    public void listFailureDomains() {
        Map<String, FailureDomain> failureDomains = admin.clusters().getFailureDomainsAsync(CONFIG_CLUSTER_NAME).join();
        Assert.assertEquals(failureDomains.size(), 0);
    }
}