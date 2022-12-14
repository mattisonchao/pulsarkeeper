package io.github.pulsarkeeper.server.cluster;

import io.github.pulsarkeeper.client.Clusters;
import io.github.pulsarkeeper.client.PulsarKeeper;
import io.github.pulsarkeeper.client.exception.PulsarKeeperClusterException;
import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;
import io.github.pulsarkeeper.server.base.MockedPulsarServiceBaseTest;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PulsarKeeperTest extends MockedPulsarServiceBaseTest {

    private PulsarKeeper pulsarKeeper;

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        internalSetup();
        setupDefaultTenantAndNamespace();
        this.pulsarKeeper = PulsarKeeper.create(PulsarKeeperOptions.builder()
                .port(pulsarKeeperPort).build());
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void list() {
        Set<String> clusters = pulsarKeeper.getClusters().list().join();
        Assert.assertEquals(clusters.size(), 1);
        Assert.assertEquals(clusters.stream().findFirst().get(), CONFIG_CLUSTER_NAME);
    }

    @Test(priority = 1)
    public void curdSuccess() {
        final String clusterName = "example";
        ClusterDataImpl clusterData = ClusterDataImpl.builder()
                .brokerServiceUrl("pulsar://127.0.0.1:6650")
                .build();
        pulsarKeeper.getClusters().create(clusterName, clusterData).join();
        ClusterData getClusterData = pulsarKeeper.getClusters().get(clusterName).join();
        Assert.assertEquals(getClusterData, clusterData);
        ClusterDataImpl newClusterData = ClusterDataImpl.builder()
                .brokerServiceUrl("pulsar://127.0.0.1:6651")
                .build();
        pulsarKeeper.getClusters().update(clusterName, newClusterData).join();
        ClusterData getNewClusterData = pulsarKeeper.getClusters().get(clusterName).join();
        Assert.assertEquals(getNewClusterData, newClusterData);
        pulsarKeeper.getClusters().delete(clusterName).join();
        Set<String> clusters = pulsarKeeper.getClusters().list().join();
        Assert.assertFalse(clusters.contains(clusterName));
    }

    @Test(priority = 1)
    public void createError() {
        // Duplicated creation
        Clusters clusters = pulsarKeeper.getClusters();
        ClusterDataImpl clusterData = ClusterDataImpl.builder()
                .brokerServiceUrl("pulsar://127.0.0.1:9999")
                .build();
        try {
            String clusterName = UUID.randomUUID().toString();
            clusters.create(clusterName, clusterData).join();
            clusters.create(clusterName, clusterData).join();
            Assert.fail("Unexpected behaviour");
        } catch (Throwable ex) {
            Assert.assertTrue(
                    ex.getCause() instanceof PulsarKeeperClusterException.ClusterDuplicatedException);
        }

        // Illegal name or data
        ClusterDataImpl illegalClusterData = ClusterDataImpl.builder()
                .serviceUrl("pulsar://127.0.0.1:9999")
                .build();
        try {
            clusters.create(UUID.randomUUID().toString(), illegalClusterData).join();
            Assert.fail("Unexpected behaviour");
        } catch (Throwable ex) {
            Assert.assertTrue(
                    ex.getCause() instanceof PulsarKeeperClusterException.IllegalClusterNameOrDataException);
        }
    }

    @Test(priority = 1)
    public void getError() {
        // Not found
        Clusters clusters = pulsarKeeper.getClusters();
        try {
            clusters.get(UUID.randomUUID().toString()).join();
            Assert.fail("Unexpected behaviour");
        } catch (Throwable ex) {
            Assert.assertTrue(
                    ex.getCause() instanceof PulsarKeeperClusterException.ClusterNotFoundException);
        }
    }

    @Test(priority = 1)
    public void updateError() {
        // Not found
        Clusters clusters = pulsarKeeper.getClusters();
        ClusterDataImpl clusterData = ClusterDataImpl.builder()
                .brokerServiceUrl("pulsar://127.0.0.1:9999")
                .build();
        try {
            clusters.update(UUID.randomUUID().toString(), clusterData).join();
            Assert.fail("Unexpected behaviour");
        } catch (Throwable ex) {
            Assert.assertTrue(
                    ex.getCause() instanceof PulsarKeeperClusterException.ClusterNotFoundException);
        }

        // Illegal name or data
        ClusterDataImpl illegalClusterData = ClusterDataImpl.builder()
                .serviceUrl("pulsar://127.0.0.1:9999")
                .build();
        try {
            clusters.update(UUID.randomUUID().toString(), illegalClusterData).join();
            Assert.fail("Unexpected behaviour");
        } catch (Throwable ex) {
            Assert.assertTrue(
                    ex.getCause() instanceof PulsarKeeperClusterException.IllegalClusterNameOrDataException);
        }
    }

    @Test(priority = 1)
    public void deleteError() {
        // Not found
        Clusters clusters = pulsarKeeper.getClusters();
        try {
            clusters.delete(UUID.randomUUID().toString()).join();
            Assert.fail("Unexpected behaviour");
        } catch (Throwable ex) {
            Assert.assertTrue(
                    ex.getCause() instanceof PulsarKeeperClusterException.ClusterNotFoundException);
        }
    }

    @Test(priority = 1)
    public void listFailureDomains() {
        Map<String, FailureDomain> failureDomains =
                pulsarKeeper.getClusters().listFailureDomains(CONFIG_CLUSTER_NAME).join();
        Assert.assertEquals(failureDomains.size(), 0);
    }
}