package io.github.pulsarkeeper.broker.handler;

import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface LookupHandler {
    void redirectToClusterOwner(@Nonnull RoutingContext ctx);
}
