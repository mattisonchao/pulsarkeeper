package io.github.pulsarkeeper.broker.service;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.apache.pulsar.common.policies.data.ClusterData;

public interface ClusterService {
    CompletableFuture<Set<String>> list();
    CompletableFuture<Optional<ClusterData>> get(@Nonnull String name);
    CompletableFuture<ClusterData> create(@Nonnull String name, @Nonnull ClusterData data);
    CompletableFuture<ClusterData> update(@Nonnull String name, @Nonnull ClusterData data);
    CompletableFuture<Void> delete(@Nonnull String name);
}
