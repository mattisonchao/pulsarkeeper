package io.github.pulsarkeeper.broker.handler;

import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface ClusterHandler {
    void list(@Nonnull RoutingContext ctx);

    void get(@Nonnull RoutingContext ctx);

    void create(@Nonnull RoutingContext ctx);

    void update(@Nonnull RoutingContext ctx);

    void delete(@Nonnull RoutingContext ctx);

    void listFailureDomains(@Nonnull RoutingContext ctx);

    void getFailureDomain(@Nonnull RoutingContext ctx);

    void createFailureDomain(@Nonnull RoutingContext ctx);

    void updateFailureDomain(@Nonnull RoutingContext ctx);

    void deleteFailureDomain(@Nonnull RoutingContext ctx);

    void listActiveBrokers(@Nonnull RoutingContext ctx);

    void getLeaderBroker(@Nonnull RoutingContext ctx);
}
