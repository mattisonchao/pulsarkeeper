package io.github.pulsarkeeper.broker.handler.impl;

import io.github.pulsarkeeper.broker.handler.AuthorizationHandler;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;

@Slf4j
@ThreadSafe
public class AuthorizationDisabledHandler implements AuthorizationHandler {
    @Override
    public void superUserPermission(RoutingContext ctx) {
        ctx.next();
    }

    @Override
    public void tenantAdminPermission(RoutingContext ctx) {
        ctx.next();
    }

    @Override
    public void tenantPermission(RoutingContext ctx, TenantOperation tenantOperation) {
        ctx.next();
    }

    @Override
    public void namespacePermission(RoutingContext ctx, NamespaceOperation namespaceOperation) {
        ctx.next();
    }

    @Override
    public void topicPermission(RoutingContext ctx, TopicOperation topicOperation) {
        ctx.next();
    }
}
