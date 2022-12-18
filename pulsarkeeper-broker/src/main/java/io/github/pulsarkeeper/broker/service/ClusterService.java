package io.github.pulsarkeeper.broker.service;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;

public interface ClusterService {
    CompletableFuture<Set<String>> list();

    CompletableFuture<Optional<ClusterData>> get(@Nonnull String name);

    CompletableFuture<ClusterData> create(@Nonnull String name, @Nonnull ClusterData data);

    CompletableFuture<ClusterData> update(@Nonnull String name, @Nonnull ClusterData data);

    CompletableFuture<Void> delete(@Nonnull String name);

    CompletableFuture<Map<String, FailureDomain>> listFailureDomains(@Nonnull String name);

    CompletableFuture<Optional<FailureDomain>> getFailureDomain(@Nonnull String clusterName,
                                                                @Nonnull String domainName);

    CompletableFuture<FailureDomain> createFailureDomain(@Nonnull String clusterName, @Nonnull String domainName,
                                                         @Nonnull FailureDomainImpl failureDomain);

    CompletableFuture<FailureDomain> updateFailureDomain(@Nonnull String clusterName, @Nonnull String domainName,
                                                         @Nonnull FailureDomainImpl failureDomain);

    CompletableFuture<Void> deleteFailureDomain(@Nonnull String clusterName, @Nonnull String domainName);

    CompletableFuture<Set<String>> listActiveBrokers();

    CompletableFuture<Optional<BrokerInfo>> getLeaderBroker();
}
