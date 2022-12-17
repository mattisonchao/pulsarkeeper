package io.github.pulsarkeeper.broker.handler;

import io.vertx.ext.web.RoutingContext;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface AuthenticationHandler{

    void handle(RoutingContext ctx);
}
