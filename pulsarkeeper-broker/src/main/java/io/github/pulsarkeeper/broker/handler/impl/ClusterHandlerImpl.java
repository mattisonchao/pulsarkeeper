package io.github.pulsarkeeper.broker.handler.impl;

import static io.github.pulsarkeeper.broker.checker.ClusterChecker.checkClusterData;
import static io.github.pulsarkeeper.broker.checker.ClusterChecker.checkClusterName;
import static io.github.pulsarkeeper.broker.handler.HandlerHelpers.badRequest;
import static io.github.pulsarkeeper.broker.handler.HandlerHelpers.conflict;
import static io.github.pulsarkeeper.broker.handler.HandlerHelpers.created;
import static io.github.pulsarkeeper.broker.handler.HandlerHelpers.noContent;
import static io.github.pulsarkeeper.broker.handler.HandlerHelpers.notFound;
import static io.github.pulsarkeeper.broker.handler.HandlerHelpers.ok;
import static io.github.pulsarkeeper.broker.handler.HandlerHelpers.remoteAddress;
import static io.github.pulsarkeeper.broker.handler.HandlerHelpers.role;
import static io.github.pulsarkeeper.broker.handler.HandlerHelpers.unprocessableEntity;
import static io.github.pulsarkeeper.common.future.CompletableFutures.unwrap;
import static org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import static org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import static org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import io.github.pulsarkeeper.broker.handler.ClusterHandler;
import io.github.pulsarkeeper.broker.resources.ClusterResourcesDelegator;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;


@Slf4j
@ThreadSafe
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
                    if (realCause instanceof NotFoundException) {
                        ok(ctx, Collections.emptySet());
                        return null;
                    }
                    ctx.fail(ex);
                    return null;
                });
    }

    @Override
    public void get(RoutingContext ctx) {
        String clusterName = ctx.pathParam("name");
        clusterResources.getAsync(clusterName)
                .thenAccept(clusterDataOpt -> {
                    if (clusterDataOpt.isEmpty()) {
                        notFound(ctx);
                        return;
                    }
                    ClusterData clusterData = clusterDataOpt.get();
                    ok(ctx, clusterData);
                }).exceptionally(ex -> {
                    log.error("[{}][{}] Failed to get cluster {} information.", remoteAddress(ctx),
                            role(ctx), clusterName, ex);
                    ctx.fail(ex);
                    return null;
                });
    }

    @Override
    public void create(RoutingContext ctx) {
        String clusterName = ctx.pathParam("name");
        ClusterDataImpl clusterData = ctx.body().asPojo(ClusterDataImpl.class);
        if (!checkClusterName(clusterName) || !checkClusterData(clusterData)) {
            badRequest(ctx);
            return;
        }
        clusterResources.getAsync(clusterName)
                .thenCompose(clusterDataOpt -> {
                    if (clusterDataOpt.isPresent()) {
                        unprocessableEntity(ctx);
                        return CompletableFuture.completedFuture(null);
                    }
                    return clusterResources.createAsync(clusterName, clusterData)
                            .thenAccept(addedClusterData -> {
                                log.info("[{}][{}] Created cluster {} with data {}.", remoteAddress(ctx), role(ctx),
                                        clusterName, addedClusterData);
                                created(ctx, addedClusterData);
                            });
                }).exceptionally(ex -> {
                    log.error("[{}][{}] Failed to create cluster {} with cluster data {}.", remoteAddress(ctx),
                            role(ctx), clusterName, clusterData, ex);
                    Throwable realCause = unwrap(ex);
                    if (realCause instanceof BadVersionException) {
                        conflict(ctx);
                        return null;
                    } else if (realCause instanceof AlreadyExistsException) {
                        unprocessableEntity(ctx);
                        return null;
                    }
                    ctx.fail(ex);
                    return null;
                });
    }

    @Override
    public void update(RoutingContext ctx) {
        String clusterName = ctx.pathParam("name");
        ClusterDataImpl clusterData = ctx.body().asPojo(ClusterDataImpl.class);
        if (!checkClusterName(clusterName) || !checkClusterData(clusterData)) {
            badRequest(ctx);
            return;
        }
        clusterResources.updateAsync(clusterName, clusterData)
                .thenAccept(updatedClusterData -> {
                    log.info("[{}][{}] Updated cluster {} with data {}.", remoteAddress(ctx), role(ctx),
                            clusterName, updatedClusterData);
                    ok(ctx, updatedClusterData);
                }).exceptionally(ex -> {
                    log.error("[{}][{}] Failed to update cluster {} with cluster data {}.", remoteAddress(ctx),
                            role(ctx), clusterName, clusterData, ex);
                    Throwable realCause = unwrap(ex);
                    if (realCause instanceof BadVersionException) {
                        conflict(ctx);
                        return null;
                    } else if (realCause instanceof NotFoundException) {
                        notFound(ctx);
                        return null;
                    }
                    ctx.fail(ex);
                    return null;
                });
    }

    @Override
    public void delete(RoutingContext ctx) {
        String clusterName = ctx.pathParam("name");
        clusterResources.deleteAsync(clusterName)
                .thenAccept(__ -> {
                    log.info("[{}][{}] Deleted cluster {}.", remoteAddress(ctx), role(ctx),
                            clusterName);
                    noContent(ctx);
                }).exceptionally(ex -> {
                    log.error("[{}][{}] Failed to delete cluster {}.", remoteAddress(ctx),
                            role(ctx), clusterName, ex);
                    Throwable realCause = unwrap(ex);
                    if (realCause instanceof BadVersionException) {
                        conflict(ctx);
                        return null;
                    } else if (realCause instanceof NotFoundException) {
                        notFound(ctx);
                        return null;
                    }
                    ctx.fail(ex);
                    return null;
                });
    }

}
