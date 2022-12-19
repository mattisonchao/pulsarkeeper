package io.github.pulsarkeeper.broker.service.impl;

import static io.github.pulsarkeeper.broker.resources.ResourcesHelpers.BASE_CLUSTERS_PATH;
import static io.github.pulsarkeeper.broker.resources.ResourcesHelpers.FAILURE_DOMAIN;
import static io.github.pulsarkeeper.broker.resources.ResourcesHelpers.joinPath;
import io.github.pulsarkeeper.broker.service.AbstractThreadPickService;
import io.github.pulsarkeeper.broker.service.ClusterService;
import io.github.pulsarkeeper.common.bean.BeanUtils;
import io.github.pulsarkeeper.common.exception.PulsarKeeperClusterException;
import io.jsonwebtoken.lang.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.metadata.api.MetadataCache;

public class ClusterServiceImpl extends AbstractThreadPickService implements ClusterService {
    private final ClusterResources clusterResources;
    private final AtomicReference<LoadManager> loadManager;
    private final LeaderElectionService leaderElectionService;


    public ClusterServiceImpl(Executor webExecutor, ClusterResources clusterResources,
                              AtomicReference<LoadManager> loadManager, LeaderElectionService leaderElectionService) {
        super(webExecutor);
        this.clusterResources = clusterResources;
        this.loadManager = loadManager;
        this.leaderElectionService = leaderElectionService;
    }

    @Override
    public CompletableFuture<Set<String>> list() {
        return pick(clusterResources::listAsync)
                .thenApply(clusters -> clusters.stream()
                        // Remove "global" cluster from returned list
                        .filter(cluster -> !Constants.GLOBAL_CLUSTER.equals(cluster))
                        .collect(Collectors.toSet()));
    }

    @Override
    public CompletableFuture<Optional<ClusterData>> get(@Nonnull String name) {
        return pick(() -> clusterResources.getClusterAsync(name));
    }

    @Override
    public CompletableFuture<ClusterData> create(@Nonnull String name, @Nonnull ClusterData data) {
        return pick(() -> clusterResources.getCache().create(joinPath(BASE_CLUSTERS_PATH, name), data))
                .thenApply(__ -> data);
    }

    @Override
    public CompletableFuture<ClusterData> update(@Nonnull String name, @Nonnull ClusterData data) {
        return pick(() -> clusterResources.getCache().readModifyUpdate(joinPath(BASE_CLUSTERS_PATH, name),
                (previous) -> {
                    ClusterDataImpl emptyData = new ClusterDataImpl();
                    BeanUtils.copyProperties(previous, emptyData);
                    BeanUtils.copyPropertiesIgnoreNull(data, emptyData);
                    return emptyData;
                }));
    }

    @Override
    public CompletableFuture<Void> delete(@Nonnull String name) {
        return pick(() -> clusterResources.getCache().delete(joinPath(BASE_CLUSTERS_PATH, name)));
    }

    @Override
    public CompletableFuture<Map<String, FailureDomain>> listFailureDomains(@Nonnull String name) {
        MetadataCache<FailureDomainImpl> failureDomainCache = clusterResources.getFailureDomainResources().getCache();
        return pick(() -> failureDomainCache.getChildren(joinPath(BASE_CLUSTERS_PATH, name, FAILURE_DOMAIN)))
                .thenCompose(domains -> {
                    final List<CompletableFuture<Optional<FailureDomainImpl>>> futures = new ArrayList<>();
                    for (String domain : domains) {
                        futures.add(failureDomainCache.get(joinPath(BASE_CLUSTERS_PATH, name, FAILURE_DOMAIN, domain)));
                    }
                    return pick(() -> CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)))
                            .thenApply(__ -> {
                                Map<String, FailureDomain> failureDomains = new HashMap<>();
                                for (int i = 0, futuresSize = futures.size(); i < futuresSize; i++) {
                                    CompletableFuture<Optional<FailureDomainImpl>> future = futures.get(i);
                                    // All futures are completed
                                    Optional<FailureDomainImpl> failureDomainOp = future.join();
                                    if (failureDomainOp.isEmpty()) {
                                        continue;
                                    }
                                    failureDomains.put(domains.get(i), failureDomainOp.get());
                                }
                                return failureDomains;
                            });
                });
    }

    @Override
    public CompletableFuture<Optional<FailureDomain>> getFailureDomain(@Nonnull String clusterName,
                                                                       @Nonnull String domainName) {
        MetadataCache<FailureDomainImpl> failureDomainCache = clusterResources.getFailureDomainResources().getCache();
        return pick(() -> failureDomainCache.get(joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN, domainName)))
                .thenApply(domainOpt -> domainOpt.map(v -> v));
    }

    @Override
    public CompletableFuture<FailureDomain> createFailureDomain(@Nonnull String clusterName, @Nonnull String domainName,
                                                                @Nonnull FailureDomainImpl failureDomain) {
        final CompletableFuture<Void> future;
        if (!Collections.isEmpty(failureDomain.getBrokers())) {
            future = checkBrokerExclusiveConflict(clusterName, failureDomain, null);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        MetadataCache<FailureDomainImpl> failureDomainCache = clusterResources.getFailureDomainResources().getCache();
        return future.thenCompose(__ -> pick(
                () -> failureDomainCache.create(joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN, domainName),
                        failureDomain)).thenApply(unused -> failureDomain));
    }

    private CompletableFuture<Void> checkBrokerExclusiveConflict(@Nonnull String clusterName,
                                                                 @Nonnull FailureDomainImpl failureDomain,
                                                                 @Nullable Set<String> whiteListDomainName) {
        // check if the broker exist in another failure domain
        return listFailureDomains(clusterName)
                .thenAccept(domains -> {
                    for (Map.Entry<String, FailureDomain> domain : domains.entrySet()) {
                        if (whiteListDomainName != null && whiteListDomainName.contains(domain.getKey())) {
                            continue;
                        }
                        Set<String> brokers = domain.getValue().getBrokers();
                        for (String broker : brokers) {
                            if (failureDomain.getBrokers().contains(broker)) {
                                throw new PulsarKeeperClusterException.FailureDomainConflictException(
                                        domain.getKey(), broker);
                            }
                        }
                    }
                });
    }

    @Override
    public CompletableFuture<FailureDomain> updateFailureDomain(@Nonnull String clusterName, @Nonnull String domainName,
                                                                @Nonnull FailureDomainImpl failureDomain) {
        MetadataCache<FailureDomainImpl> failureDomainCache = clusterResources.getFailureDomainResources().getCache();
        return checkBrokerExclusiveConflict(clusterName, failureDomain, Set.of(domainName))
                .thenCompose(__ ->
                        pick(() -> failureDomainCache.readModifyUpdate(
                                joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN, domainName),
                                old -> {
                                    FailureDomainImpl emptyData = new FailureDomainImpl();
                                    BeanUtils.copyProperties(old, emptyData);
                                    BeanUtils.copyPropertiesIgnoreNull(failureDomain, emptyData);
                                    return emptyData;
                                }))).thenApply(v -> v);

    }

    @Override
    public CompletableFuture<Void> deleteFailureDomain(@Nonnull String clusterName, @Nonnull String domainName) {
        MetadataCache<FailureDomainImpl> failureDomainCache = clusterResources.getFailureDomainResources().getCache();
        return pick(
                () -> failureDomainCache.delete(joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN, domainName)));
    }

    @Override
    public CompletableFuture<Set<String>> listActiveBrokers() {
        return loadManager.get().getAvailableBrokersAsync();
    }

    @Override
    public CompletableFuture<Optional<BrokerInfo>> getLeaderBroker() {
        return leaderElectionService.readCurrentLeader()
                .thenApply(leaderOpt -> leaderOpt.map(leader -> BrokerInfo.builder()
                        .serviceUrl(leader.getServiceUrl())
                        .build()));
    }

}
