package io.github.pulsarkeeper.server.handler;

import io.vertx.ext.web.RoutingContext;

public interface AuthenticationHandler{

    void handle(RoutingContext ctx);
}
