package io.github.pulsarkeeper.server.resources;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.pulsar.broker.resources.ClusterResources;

public class ClusterResourcesDelegator extends AbstractThreadPickResources {
    private final ClusterResources clusterResources;

    public ClusterResourcesDelegator(ClusterResources clusterResources, Executor ioExecutor) {
        super(ioExecutor);
        this.clusterResources = clusterResources;
    }

    public CompletableFuture<Set<String>> listAsync() {
        return pick(clusterResources::listAsync);
    }
}
