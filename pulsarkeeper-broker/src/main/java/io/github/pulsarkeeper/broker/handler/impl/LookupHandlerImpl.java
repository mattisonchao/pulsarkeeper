package io.github.pulsarkeeper.broker.handler.impl;

import static io.github.pulsarkeeper.broker.handler.HandlerHelpers.notFound;
import io.github.pulsarkeeper.broker.handler.LookupHandler;
import io.github.pulsarkeeper.broker.service.LookupService;
import io.github.pulsarkeeper.common.exception.PulsarKeeperClusterException;
import io.github.pulsarkeeper.common.future.CompletableFutures;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;

@Slf4j
@ThreadSafe
public class LookupHandlerImpl implements LookupHandler {
    private final LookupService lookupService;
    private final ServiceConfiguration pulsarConfiguration;

    public LookupHandlerImpl(LookupService lookupService, ServiceConfiguration pulsarConfiguration) {
        this.lookupService = lookupService;
        this.pulsarConfiguration = pulsarConfiguration;
    }

    @Override
    public void redirectToClusterOwner(@Nonnull RoutingContext ctx) {
        String clusterName = ctx.pathParam("clusterName");
        if (pulsarConfiguration.getClusterName().equalsIgnoreCase(clusterName)) {
            ctx.next();
            return;
        }
        lookupService.lookupCluster(clusterName)
                .thenAccept(lookupData -> {
                    ctx.redirect(ctx.request().isSSL() ? lookupData.getServiceTlsURL() : lookupData.getServiceURL());
                }).exceptionally(ex -> {
                    Throwable realCause = CompletableFutures.unwrap(ex);
                    if (realCause instanceof PulsarKeeperClusterException.ClusterNotFoundException) {
                        notFound(ctx);
                        return null;
                    }
                    ctx.fail(ex);
                    return null;
                });
    }
}
