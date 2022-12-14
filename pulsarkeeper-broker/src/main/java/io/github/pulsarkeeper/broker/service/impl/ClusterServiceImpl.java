package io.github.pulsarkeeper.broker.service.impl;

import io.github.pulsarkeeper.broker.resources.ResourcesHelpers;
import io.github.pulsarkeeper.broker.service.AbstractThreadPickService;
import io.github.pulsarkeeper.broker.service.ClusterService;
import io.github.pulsarkeeper.common.bean.BeanUtils;
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
        return pick(() -> clusterResources.getCache().create(
                ResourcesHelpers.joinPath(ResourcesHelpers.BASE_CLUSTERS_PATH, name), data))
                .thenApply(__ -> data);
    }

    @Override
    public CompletableFuture<ClusterData> update(@Nonnull String name, @Nonnull ClusterData data) {
        return pick(() -> clusterResources.getCache()
                .readModifyUpdate(ResourcesHelpers.joinPath(ResourcesHelpers.BASE_CLUSTERS_PATH, name),
                        (previous) -> {
                            ClusterDataImpl emptyData = new ClusterDataImpl();
                            BeanUtils.copyProperties(previous, emptyData);
                            BeanUtils.copyPropertiesIgnoreNull(data, emptyData);
                            return emptyData;
                        }));
    }

    @Override
    public CompletableFuture<Void> delete(@Nonnull String name) {
        return pick(() -> clusterResources.getCache().delete(
                ResourcesHelpers.joinPath(ResourcesHelpers.BASE_CLUSTERS_PATH, name)));
    }

}
