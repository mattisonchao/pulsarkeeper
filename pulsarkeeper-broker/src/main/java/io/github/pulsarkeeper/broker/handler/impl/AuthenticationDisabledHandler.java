package io.github.pulsarkeeper.broker.handler.impl;

import io.github.pulsarkeeper.broker.handler.AuthenticationHandler;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ThreadSafe
public class AuthenticationDisabledHandler implements AuthenticationHandler {
    @Override
    public void handle(RoutingContext ctx) {
        ctx.next();
    }
}
