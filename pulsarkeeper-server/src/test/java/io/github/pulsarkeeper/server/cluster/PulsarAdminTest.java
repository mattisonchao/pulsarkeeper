package io.github.pulsarkeeper.server.cluster;

import io.github.pulsarkeeper.server.base.MockedPulsarServiceBaseTest;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
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
    public void getAll() throws PulsarAdminException {
        List<String> clusters = admin.clusters().getClusters();
        Assert.assertEquals(clusters.size(), 1);
        Assert.assertEquals(clusters.stream().findFirst().get(), CONFIG_CLUSTER_NAME);
    }
}