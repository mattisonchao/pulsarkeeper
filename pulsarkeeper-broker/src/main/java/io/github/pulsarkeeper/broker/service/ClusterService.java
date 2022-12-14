package io.github.pulsarkeeper.broker.service;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;

public interface ClusterService {
    CompletableFuture<Set<String>> list();

    CompletableFuture<Optional<ClusterData>> get(@Nonnull String name);

    CompletableFuture<ClusterData> create(@Nonnull String name, @Nonnull ClusterData data);

    CompletableFuture<ClusterData> update(@Nonnull String name, @Nonnull ClusterData data);

    CompletableFuture<Void> delete(@Nonnull String name);

    CompletableFuture<Map<String, FailureDomain>> listFailureDomains(@Nonnull String name);
}
