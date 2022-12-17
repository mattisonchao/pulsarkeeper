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
import io.github.pulsarkeeper.broker.service.ClusterService;
import io.vertx.ext.web.RoutingContext;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;


@Slf4j
@ThreadSafe
public class ClusterHandlerImpl implements ClusterHandler {
    private final ClusterService clusterService;

    public ClusterHandlerImpl(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public void list(@Nonnull RoutingContext ctx) {
        clusterService.list()
                .thenAccept(clusters -> ok(ctx, clusters))
                .exceptionally(ex -> {
                    log.error("[{}][{}] Failed to get list of cluster.", remoteAddress(ctx), role(ctx), ex);
                    ctx.fail(ex);
                    return null;
                });
    }

    @Override
    public void get(@Nonnull RoutingContext ctx) {
        String clusterName = ctx.pathParam("name");
        clusterService.get(clusterName)
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
    public void create(@Nonnull RoutingContext ctx) {
        String clusterName = ctx.pathParam("name");
        ClusterDataImpl clusterData = ctx.body().asPojo(ClusterDataImpl.class);
        if (!checkClusterName(clusterName) || !checkClusterData(clusterData)) {
            badRequest(ctx);
            return;
        }
        clusterService.get(clusterName)
                .thenCompose(clusterDataOpt -> {
                    if (clusterDataOpt.isPresent()) {
                        unprocessableEntity(ctx);
                        return CompletableFuture.completedFuture(null);
                    }
                    return clusterService.create(clusterName, clusterData)
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
    public void update(@Nonnull RoutingContext ctx) {
        String clusterName = ctx.pathParam("name");
        ClusterDataImpl clusterData = ctx.body().asPojo(ClusterDataImpl.class);
        if (!checkClusterName(clusterName) || !checkClusterData(clusterData)) {
            badRequest(ctx);
            return;
        }
        clusterService.update(clusterName, clusterData)
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
    public void delete(@Nonnull RoutingContext ctx) {
        String clusterName = ctx.pathParam("name");
        clusterService.delete(clusterName)
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

    @Override
    public void listFailureDomains(@Nonnull RoutingContext ctx) {
        String clusterName = ctx.pathParam("clusterName");
        clusterService.listFailureDomains(clusterName)
                .thenAccept(failureDomains -> ok(ctx, failureDomains))
                .exceptionally(ex -> {
                    log.error("[{}][{}] Failed to get list of failure domain.", remoteAddress(ctx), role(ctx), ex);
                    ctx.fail(ex);
                    return null;
                });
    }

    @Override
    public void getFailureDomain(@Nonnull RoutingContext ctx) {

    }

    @Override
    public void setFailureDomain(@Nonnull RoutingContext ctx) {

    }

    @Override
    public void deleteFailureDomain(@Nonnull RoutingContext ctx) {

    }
}
