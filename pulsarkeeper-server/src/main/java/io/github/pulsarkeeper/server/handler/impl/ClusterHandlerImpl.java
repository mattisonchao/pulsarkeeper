package io.github.pulsarkeeper.server.handler.impl;

import static io.github.pulsarkeeper.common.future.CompletableFutures.unwrap;
import static io.github.pulsarkeeper.server.handler.HandlerHelpers.ok;
import static io.github.pulsarkeeper.server.handler.HandlerHelpers.remoteAddress;
import static io.github.pulsarkeeper.server.handler.HandlerHelpers.role;
import io.github.pulsarkeeper.server.handler.ClusterHandler;
import io.github.pulsarkeeper.server.resources.ClusterResourcesDelegator;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.metadata.api.MetadataStoreException;


@Slf4j
public class ClusterHandlerImpl implements ClusterHandler {
    private final ClusterResourcesDelegator clusterResources;

    public ClusterHandlerImpl(ClusterResourcesDelegator clusterResources) {
        this.clusterResources = clusterResources;
    }

    @Override
    public void list(RoutingContext ctx) {
        clusterResources.listAsync()
                .thenApply(clusters -> clusters.stream()
                        // Remove "global" cluster from returned list
                        .filter(cluster -> !Constants.GLOBAL_CLUSTER.equals(cluster))
                        .collect(Collectors.toSet()))
                .thenAccept(clusters -> ok(ctx, clusters))
                .exceptionally(ex -> {
                    log.error("[{}][{}] Failed to get list of cluster.", remoteAddress(ctx), role(ctx), ex);
                    Throwable realCause = unwrap(ex);
                    if (realCause instanceof MetadataStoreException.NotFoundException) {
                        ok(ctx, Collections.emptySet());
                        return null;
                    }
                    ctx.fail(ex);
                    return null;
                });
    }

}
