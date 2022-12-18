package io.github.pulsarkeeper.broker.service.impl;

import io.github.pulsarkeeper.broker.service.ClusterService;
import io.github.pulsarkeeper.broker.service.LookupService;
import io.github.pulsarkeeper.common.api.lookup.LookupData;
import io.github.pulsarkeeper.common.exception.PulsarKeeperClusterException;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.apache.pulsar.common.policies.data.ClusterData;

public class LookupServiceImpl implements LookupService {
    private final ClusterService clusterService;

    public LookupServiceImpl(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public CompletableFuture<LookupData> lookupCluster(@Nonnull String clusterName) {
        return clusterService.get(clusterName)
                .thenApply(clusterOpt -> {
                    if (clusterOpt.isEmpty()) {
                        throw new PulsarKeeperClusterException.ClusterNotFoundException(clusterName);
                    }
                    ClusterData clusterData = clusterOpt.get();
                    return LookupData.builder()
                            .serviceURL(clusterData.getServiceUrl())
                            .serviceTlsURL(clusterData.getServiceUrlTls())
                            .brokerServiceURL(clusterData.getBrokerServiceUrl())
                            .brokerServiceTlsURL(clusterData.getServiceUrlTls())
                            .build();
                });
    }
}
