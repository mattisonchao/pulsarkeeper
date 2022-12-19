package io.github.pulsarkeeper.server.cluster;

import io.github.pulsarkeeper.client.Clusters;
import io.github.pulsarkeeper.client.PulsarKeeper;
import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;
import io.github.pulsarkeeper.common.exception.PulsarKeeperClusterException;
import io.github.pulsarkeeper.server.base.MockedPulsarServiceBaseTest;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
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
        String clusterName = UUID.randomUUID().toString();
        try {
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
        clusters.delete(clusterName).join();
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

    @Test(priority = 1)
    public void curdSuccessFailureDomain() {
        String clusterName = "test";
        String domainName = "test-domain";
        FailureDomainImpl failureDomain = new FailureDomainImpl();
        failureDomain.setBrokers(Set.of("broker-1", "broker-2"));
        FailureDomain createdFailureDomain =
                pulsarKeeper.getClusters().createFailureDomains(clusterName, domainName, failureDomain).join();
        Assert.assertEquals(createdFailureDomain, failureDomain);
        FailureDomain receivedDomain = pulsarKeeper.getClusters().getFailureDomain(clusterName, domainName).join();
        Assert.assertEquals(failureDomain, receivedDomain);

        FailureDomainImpl updateFailureDomain = new FailureDomainImpl();
        updateFailureDomain.setBrokers(Set.of("broker-1"));
        FailureDomain updatedFailureDomain =
                pulsarKeeper.getClusters().updateFailureDomains(clusterName, domainName, updateFailureDomain).join();
        Assert.assertEquals(updateFailureDomain, updatedFailureDomain);
        pulsarKeeper.getClusters().deleteFailureDomains(clusterName, domainName).join();
        try {
            pulsarKeeper.getClusters().getFailureDomain(clusterName, domainName).join();
            Assert.fail();
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getCause() instanceof PulsarKeeperClusterException.FailureDomainNotFoundException);
        }
    }

    @Test(priority = 1)
    public void createFailureDomainError() {
        String clusterName = "test";
        String domainName = "test-domain";
        String domainName2 = "test-domain-2";
        FailureDomainImpl failureDomain1 = new FailureDomainImpl();
        failureDomain1.setBrokers(Set.of("broker-1", "broker-2"));
        pulsarKeeper.getClusters().createFailureDomains(clusterName, domainName, failureDomain1).join();
        try {
            pulsarKeeper.getClusters().createFailureDomains(clusterName, domainName, failureDomain1).join();
            Assert.fail();
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getCause() instanceof PulsarKeeperClusterException.FailureDomainConflictException);
        }
        try {
            FailureDomainImpl failureDomain2 = new FailureDomainImpl();
            failureDomain2.setBrokers(Set.of("broker-1"));
            pulsarKeeper.getClusters().createFailureDomains(clusterName, domainName2, failureDomain2).join();
            Assert.fail();
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getCause() instanceof PulsarKeeperClusterException.FailureDomainConflictException);
        }
        pulsarKeeper.getClusters().deleteFailureDomains(clusterName, domainName).join();
    }

    @Test(priority = 1)
    public void updateFailureDomainError() {
        String clusterName = "test";
        String domainName = "test-domain";
        String domainName2 = "test-domain2";
        FailureDomainImpl failureDomain1 = new FailureDomainImpl();
        failureDomain1.setBrokers(Set.of("broker-1", "broker-2"));
        try {
            pulsarKeeper.getClusters().updateFailureDomains(clusterName, domainName, failureDomain1).join();
            Assert.fail();
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getCause() instanceof PulsarKeeperClusterException.FailureDomainNotFoundException);
        }
        FailureDomainImpl failureDomain2 = new FailureDomainImpl();
        failureDomain2.setBrokers(Set.of("broker-3", "broker-4"));
        pulsarKeeper.getClusters().createFailureDomains(clusterName, domainName, failureDomain1).join();
        pulsarKeeper.getClusters().createFailureDomains(clusterName, domainName2, failureDomain2).join();

        try {
            pulsarKeeper.getClusters().updateFailureDomains(clusterName, domainName, failureDomain2).join();
            Assert.fail();
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getCause() instanceof PulsarKeeperClusterException.FailureDomainConflictException);
        }

        pulsarKeeper.getClusters().deleteFailureDomains(clusterName, domainName).join();
        pulsarKeeper.getClusters().deleteFailureDomains(clusterName, domainName2).join();
    }

    @Test(priority = 1)
    public void deleteFailureDomainError() {
        String clusterName = "test";
        String domainName = "test-domain";
        try {
            pulsarKeeper.getClusters().deleteFailureDomains(clusterName, domainName).join();
            Assert.fail();
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getCause() instanceof PulsarKeeperClusterException.FailureDomainNotFoundException);
        }
    }

    @Test
    public void getActiveBrokers() {
        String clusterName = "test";
        Set<String> activeBrokers = pulsarKeeper.getClusters().listActiveBrokers(clusterName).join();
        Assert.assertEquals(activeBrokers.size(), 1);
        URL url = PulsarKeeperTest.this.brokerUrl;
        Assert.assertEquals(activeBrokers.stream().findFirst().get(), url.getHost() + ":" + url.getPort());
    }

    @Test
    public void getLeaderBroker() {
        String clusterName = "test";
        BrokerInfo brokerInfo = pulsarKeeper.getClusters().getLeaderBroker(clusterName).join();
        String serviceUrl = brokerInfo.getServiceUrl();
        Assert.assertEquals(serviceUrl, brokerUrl.toString());
    }
}