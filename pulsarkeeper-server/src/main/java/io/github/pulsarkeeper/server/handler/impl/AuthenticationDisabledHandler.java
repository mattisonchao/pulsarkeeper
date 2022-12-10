package io.github.pulsarkeeper.server.handler.impl;

import io.github.pulsarkeeper.server.handler.AuthenticationHandler;
import io.vertx.ext.web.RoutingContext;

public class AuthenticationDisabledHandler implements AuthenticationHandler {
    @Override
    public void handle(RoutingContext ctx) {
        ctx.next();
    }
}
