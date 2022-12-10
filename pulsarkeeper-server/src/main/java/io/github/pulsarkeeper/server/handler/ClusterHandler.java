package io.github.pulsarkeeper.server.handler;

import io.vertx.ext.web.RoutingContext;

public interface ClusterHandler {

    void list(RoutingContext ctx);

}
