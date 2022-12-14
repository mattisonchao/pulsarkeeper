package io.github.pulsarkeeper.broker.handler;

import io.vertx.ext.web.RoutingContext;

public interface AuthenticationHandler{

    void handle(RoutingContext ctx);
}
