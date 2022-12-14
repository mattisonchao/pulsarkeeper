package io.github.pulsarkeeper.broker.handler;

import io.vertx.ext.web.RoutingContext;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface ClusterHandler {

    void list(RoutingContext ctx);
    void get(RoutingContext ctx);
    void create(RoutingContext ctx);
    void update(RoutingContext ctx);
    void delete(RoutingContext ctx);
}
