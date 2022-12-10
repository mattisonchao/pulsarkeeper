package io.github.pulsarkeeper.server.cluster;

import io.github.pulsarkeeper.client.PulsarKeeper;
import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;
import io.github.pulsarkeeper.server.base.MockedPulsarServiceBaseTest;
import java.util.Set;
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

    @Test
    public void curdSuccess() {
        final String clusterName = "example";
        ClusterDataImpl clusterData = ClusterDataImpl.builder()
                .serviceUrl("pulsar://127.0.0.1:6650")
                .build();
        pulsarKeeper.getCluster().create(clusterName, clusterData).join();
        ClusterData getClusterData = pulsarKeeper.getCluster().get(clusterName).join();
        Assert.assertEquals(getClusterData, clusterData);
        pulsarKeeper.getCluster().delete(clusterName).join();
        Set<String> clusters = pulsarKeeper.getCluster().list().join();
        Assert.assertFalse(clusters.contains(clusterName));
    }

    @Test
    public void createError() {

    }

    @Test
    public void getError() {

    }

    @Test
    public void updateError() {

    }

    @Test
    public void deleteError() {

    }

}