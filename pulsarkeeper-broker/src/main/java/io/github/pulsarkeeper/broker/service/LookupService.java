package io.github.pulsarkeeper.broker.service;

import io.github.pulsarkeeper.common.api.lookup.LookupData;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public interface LookupService {
    /**
     * Lookup cluster URL by cluster name
     *
     * @param clusterName cluster name
     * @return lookup data
     * @throws io.github.pulsarkeeper.common.exception.PulsarKeeperClusterException.ClusterNotFoundException
     */
    CompletableFuture<LookupData> lookupCluster(@Nonnull String clusterName);
}
