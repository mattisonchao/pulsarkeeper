package io.github.pulsarkeeper.server.resources;

import static io.github.pulsarkeeper.server.resources.ResourcesHelpers.BASE_CLUSTERS_PATH;
import static io.github.pulsarkeeper.server.resources.ResourcesHelpers.joinPath;
import io.github.pulsarkeeper.common.bean.BeanUtils;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;

public class ClusterResourcesDelegator extends AbstractThreadPickResources {
    private final ClusterResources clusterResources;

    public ClusterResourcesDelegator(ClusterResources clusterResources, Executor ioExecutor) {
        super(ioExecutor);
        this.clusterResources = clusterResources;
    }

    public CompletableFuture<Set<String>> listAsync() {
        return pick(clusterResources::listAsync);
    }

    public CompletableFuture<Optional<ClusterData>> getAsync(@Nonnull String clusterName) {
        return pick(() -> clusterResources.getClusterAsync(clusterName));
    }

    public CompletableFuture<ClusterData> createAsync(String clusterName, ClusterDataImpl clusterData) {
        return pick(() -> clusterResources.getCache().create(joinPath(BASE_CLUSTERS_PATH, clusterName), clusterData))
                .thenApply(__ -> clusterData);
    }

    public CompletableFuture<ClusterData> updateAsync(String clusterName, ClusterDataImpl clusterData) {
        return pick(() -> clusterResources.getCache()
                .readModifyUpdate(joinPath(BASE_CLUSTERS_PATH, clusterName), (previous) -> {
                    ClusterDataImpl emptyData = new ClusterDataImpl();
                    BeanUtils.copyProperties(previous, emptyData);
                    BeanUtils.copyPropertiesIgnoreNull(clusterData, emptyData);
                    return emptyData;
                }));
    }

    public CompletionStage<Void> deleteAsync(String clusterName) {
        return pick(() -> clusterResources.getCache().delete(joinPath(BASE_CLUSTERS_PATH, clusterName)));
    }
}
