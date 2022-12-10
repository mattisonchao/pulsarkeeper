package io.github.pulsarkeeper.server.handler;

import io.vertx.ext.web.RoutingContext;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;

public interface AuthorizationHandler {

    void superUserPermission(RoutingContext ctx);

    void tenantAdminPermission(RoutingContext ctx);

    void tenantPermission(RoutingContext ctx, TenantOperation tenantOperation);

    void namespacePermission(RoutingContext ctx, NamespaceOperation namespaceOperation);

    void topicPermission(RoutingContext ctx, TopicOperation topicOperation);
}
