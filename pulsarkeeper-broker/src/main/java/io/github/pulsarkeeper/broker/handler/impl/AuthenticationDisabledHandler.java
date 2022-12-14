package io.github.pulsarkeeper.broker.handler.impl;

import io.github.pulsarkeeper.broker.handler.AuthenticationHandler;
import io.vertx.ext.web.RoutingContext;

public class AuthenticationDisabledHandler implements AuthenticationHandler {
    @Override
    public void handle(RoutingContext ctx) {
        ctx.next();
    }
}
