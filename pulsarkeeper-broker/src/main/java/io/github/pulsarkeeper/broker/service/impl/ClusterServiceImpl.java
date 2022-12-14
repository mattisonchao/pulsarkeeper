package io.github.pulsarkeeper.broker.service.impl;

import static io.github.pulsarkeeper.broker.resources.ResourcesHelpers.BASE_CLUSTERS_PATH;
import static io.github.pulsarkeeper.broker.resources.ResourcesHelpers.FAILURE_DOMAIN;
import static io.github.pulsarkeeper.broker.resources.ResourcesHelpers.joinPath;
import io.github.pulsarkeeper.broker.service.AbstractThreadPickService;
import io.github.pulsarkeeper.broker.service.ClusterService;
import io.github.pulsarkeeper.common.bean.BeanUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.metadata.api.MetadataCache;

public class ClusterServiceImpl extends AbstractThreadPickService implements ClusterService {
    private final ClusterResources clusterResources;


    public ClusterServiceImpl(Executor webExecutor, ClusterResources clusterResources) {
        super(webExecutor);
        this.clusterResources = clusterResources;
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

}
