package io.github.pulsarkeeper.server.cluster;

import io.github.pulsarkeeper.client.Cluster;
import io.github.pulsarkeeper.client.PulsarKeeper;
import io.github.pulsarkeeper.client.exception.PulsarKeeperClusterException;
import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;
import io.github.pulsarkeeper.server.base.MockedPulsarServiceBaseTest;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PulsarKeeperTest extends MockedPulsarServiceBaseTest {

    private PulsarKeeper pulsarKeeper;

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
        this.pulsarKeeper = PulsarKeeper.create(PulsarKeeperOptions.builder()
                .build());
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void list() {
        Set<String> clusters = pulsarKeeper.getCluster().list().join();
        Assert.assertEquals(clusters.size(), 1);
        Assert.assertEquals(clusters.stream().findFirst().get(), CONFIG_CLUSTER_NAME);
    }

    @Test(priority = 1)
    public void curdSuccess() {
        final String clusterName = "example";
        ClusterDataImpl clusterData = ClusterDataImpl.builder()
                .brokerServiceUrl("pulsar://127.0.0.1:6650")
                .build();
        pulsarKeeper.getCluster().create(clusterName, clusterData).join();
        ClusterData getClusterData = pulsarKeeper.getCluster().get(clusterName).join();
        Assert.assertEquals(getClusterData, clusterData);
        pulsarKeeper.getCluster().delete(clusterName).join();
        Set<String> clusters = pulsarKeeper.getCluster().list().join();
        Assert.assertFalse(clusters.contains(clusterName));
    }

    @Test(priority = 1)
    public void createError() {
        // Duplicated creation
        Cluster cluster = pulsarKeeper.getCluster();
        ClusterDataImpl clusterData = ClusterDataImpl.builder()
                .brokerServiceUrl("pulsar://127.0.0.1:9999")
                .build();
        try {
            String clusterName = UUID.randomUUID().toString();
            cluster.create(clusterName, clusterData).join();
            cluster.create(clusterName, clusterData).join();
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
            cluster.create(UUID.randomUUID().toString(), illegalClusterData).join();
            Assert.fail("Unexpected behaviour");
        } catch (Throwable ex) {
            Assert.assertTrue(
                    ex.getCause() instanceof PulsarKeeperClusterException.IllegalClusterNameOrDataException);
        }
    }

    @Test(priority = 1)
    public void getError() {
        // Not found
        Cluster cluster = pulsarKeeper.getCluster();
        try {
            cluster.get(UUID.randomUUID().toString()).join();
            Assert.fail("Unexpected behaviour");
        } catch (Throwable ex) {
            Assert.assertTrue(
                    ex.getCause() instanceof PulsarKeeperClusterException.ClusterNotFoundException);
        }
    }

    @Test(priority = 1)
    public void updateError() {
        // Not found
        Cluster cluster = pulsarKeeper.getCluster();
        ClusterDataImpl clusterData = ClusterDataImpl.builder()
                .brokerServiceUrl("pulsar://127.0.0.1:9999")
                .build();
        try {
            cluster.update(UUID.randomUUID().toString(), clusterData).join();
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
            cluster.update(UUID.randomUUID().toString(), illegalClusterData).join();
            Assert.fail("Unexpected behaviour");
        } catch (Throwable ex) {
            Assert.assertTrue(
                    ex.getCause() instanceof PulsarKeeperClusterException.IllegalClusterNameOrDataException);
        }
    }

    @Test(priority = 1)
    public void deleteError() {
        // Not found
        Cluster cluster = pulsarKeeper.getCluster();
        try {
            cluster.delete(UUID.randomUUID().toString()).join();
            Assert.fail("Unexpected behaviour");
        } catch (Throwable ex) {
            Assert.assertTrue(
                    ex.getCause() instanceof PulsarKeeperClusterException.ClusterNotFoundException);
        }
    }

}