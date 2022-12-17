package io.github.pulsarkeeper.broker;

import io.github.pulsarkeeper.broker.handler.AuthenticationHandler;
import io.github.pulsarkeeper.broker.handler.AuthorizationHandler;
import io.github.pulsarkeeper.broker.handler.ClusterHandler;
import io.github.pulsarkeeper.broker.handler.ClusterPoliciesHandler;
import io.github.pulsarkeeper.broker.handler.impl.AuthenticationDisabledHandler;
import io.github.pulsarkeeper.broker.handler.impl.AuthorizationDisabledHandler;
import io.github.pulsarkeeper.broker.handler.impl.ClusterHandlerImpl;
import io.github.pulsarkeeper.broker.handler.impl.ClusterPoliciesHandlerImpl;
import io.github.pulsarkeeper.broker.options.PulsarKeeperServerOptions;
import io.github.pulsarkeeper.broker.service.impl.ClusterServiceImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerService;

@Slf4j
public class PulsarKeeperServer extends AbstractVerticle {
    public static final String SIGNATURE = "pulsarKeeper";
    private final BrokerService brokerService;
    private final PulsarKeeperServerOptions options;
    private ClusterHandler clusterHandler;
    private ClusterPoliciesHandler clusterPoliciesHandler;
    private AuthorizationHandler authorizationHandler;
    private AuthenticationHandler authenticationHandler;

    @SneakyThrows
    public PulsarKeeperServer(BrokerService brokerService) {
        this.brokerService = brokerService;
        this.options = PulsarKeeperServerOptions.fromProperties(brokerService.getPulsar().getConfig().getProperties());
    }

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        PulsarResources pulsarResources = brokerService.getPulsar().getPulsarResources();
        ClusterServiceImpl clusterService =
                new ClusterServiceImpl(vertx.nettyEventLoopGroup(), pulsarResources.getClusterResources());
        this.clusterHandler = new ClusterHandlerImpl(clusterService);
        this.clusterPoliciesHandler = new ClusterPoliciesHandlerImpl(clusterService);
        this.authorizationHandler = new AuthorizationDisabledHandler();
        this.authenticationHandler = new AuthenticationDisabledHandler();
    }

    @Override
    public void start(Promise<Void> promise) {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.route()
                .handler(BodyHandler.create())
                .handler(authenticationHandler::handle);
        loadV1ClusterEndpoint(router);
        loadV1ClusterPoliciesEndpoint(router);
        loadV1BrokerEndpoint(router);
        loadV1DynamicConfEndpoint(router);
        loadV1NamespacesEndpoint(router);
        loadV1NamespacePoliciesEndpoint(router);
        loadV1TopicEndpoint(router);
        loadV1TopicPoliciesEndpoint(router);
        loadV1MetadataEndpoint(router);
        loadV1SchemaEndpoint(router);
        loadV1MessagesEndpoint(router);
        loadPulsarAdminClusterEndpoint(router);
        server.requestHandler(router)
                .listen(options.getPort())
                .onSuccess(event -> promise.complete())
                .onFailure(promise::fail);
    }

    private void loadV1NamespacesEndpoint(Router router) {
        router.get("/api/v1/namespaces");
        router.get("/api/v1/namespaces/:tenant");
        router.post("/api/v1/namespaces/:tenant");
        router.patch("/api/v1/namespaces/:tenant");
        router.delete("/api/v1/namespaces/:tenant");
        router.get("/api/v1/namespaces/:tenant/:local");
        router.post("/api/v1/namespaces/:tenant/:local");
        router.patch("/api/v1/namespaces/:tenant/:local");
        router.delete("/api/v1/namespaces/:tenant/:local");
        router.get("/api/v1/namespaces/:tenant/:local/topics");
    }

    private void loadV1NamespacePoliciesEndpoint(Router router) {
        router.get("/api/v1/namespaces/:tenant/:local/policies");
        router.get("/api/v1/namespaces/:tenant/:local/policies/:policyName");
        router.patch("/api/v1/namespaces/:tenant/:local/policies");
        router.delete("/api/v1/namespaces/:tenant/:local/policies");
    }

    private void loadV1SchemaEndpoint(Router router) {
        router.get("/api/v1/schemas/:tenant/:local/:name");
        router.post("/api/v1/schemas/:tenant/:local/:name");
        router.put("/api/v1/schemas/:tenant/:local/:name");
        router.delete("/api/v1/schemas/:tenant/:local/:name");
    }

    private void loadV1TopicEndpoint(Router router) {
        router.get("/api/v1/topics/:domain/:tenant/:local/:name");
        router.post("/api/v1/topics/:domain/:tenant/:local/:name");
        router.patch("/api/v1/topics/:domain/:tenant/:local/:name");
        router.delete("/api/v1/topics/:domain/:tenant/:local/:name");
        router.post("/api/v1/topics/:domain/:tenant/:local/:name/actions/unload");
        router.post("/api/v1/topics/:domain/:tenant/:local/:name/actions/offload");
        router.post("/api/v1/topics/:domain/:tenant/:local/:name/actions/truncate");
        router.post("/api/v1/topics/:domain/:tenant/:local/:name/actions/cursors/reset");
    }

    private void loadV1MessagesEndpoint(Router router) {
        router.get("/api/v1/messages/:domain/:tenant/:local/:name");
        router.post("/api/v1/messages/:domain/:tenant/:local/:name");
    }

    private void loadV1TopicPoliciesEndpoint(Router router) {
        router.get("/api/v1/topics/:domain/:tenant/:local/:name/policies");
        router.get("/api/v1/topics/:domain/:tenant/:local/:name/policies/:policyName");
        router.patch("/api/v1/topics/:domain/:tenant/:local/:name/policies");
        router.delete("/api/v1/topics/:domain/:tenant/:local/:name/policies");
    }

    private void loadV1MetadataEndpoint(Router router) {
        router.get("/api/v1/meta/unsafe/get");
        router.delete("/api/v1/meta/unsafe/delete");
    }

    private void loadV1DynamicConfEndpoint(Router router) {
        router.get("/api/v1/configurations/dynamic");
        router.get("/api/v1/configurations/dynamic/:confName");
        router.patch("/api/v1/configurations/dynamic");
        router.delete("/api/v1/configurations/dynamic");
    }

    private void loadV1BrokerEndpoint(Router router) {
        router.get("/api/v1/brokers");
        router.get("/api/v1/brokers/configurations");
        router.post("/api/v1/brokers/actions/shutdown");
        router.post("/api/v1/brokers/actions/checks/ready");
        router.post("/api/v1/brokers/actions/checks/health");
    }

    private void loadV1ClusterPoliciesEndpoint(Router router) {
        router.get("/api/v1/clusters/:clusterName/policies/isolation/namespaces");
        router.get("/api/v1/clusters/:clusterName/policies/isolation/namespaces/:isolationGroup");
        router.post("/api/v1/clusters/:clusterName/policies/isolation/namespaces/:isolationGroup");
        router.patch("/api/v1/clusters/:clusterName/policies/isolation/namespaces/:isolationGroup");
        router.delete("/api/v1/clusters/:clusterName/policies/isolation/namespaces/:isolationGroup");
    }

    private void loadV1ClusterEndpoint(Router router) {
        router.get("/api/v1/clusters")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::list);
        router.get("/api/v1/clusters/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::get);
        router.post("/api/v1/clusters/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::create);
        router.patch("/api/v1/clusters/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::update);
        router.delete("/api/v1/clusters/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::delete);
        router.get("/api/v1/clusters/:clusterName/domains/failure")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::listFailureDomains);
        router.get("/api/v1/clusters/:clusterName/domains/failure/:domainName")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::getFailureDomain);
        router.post("/api/v1/clusters/:clusterName/domains/failure/:domainName")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::setFailureDomain);
        router.patch("/api/v1/clusters/:clusterName/domains/failure/:domainName")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::setFailureDomain);
        router.delete("/api/v1/clusters/:clusterName/domains/failure/:domainName")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::deleteFailureDomain);
        router.get("/api/v1/clusters/:clusterName/brokers/active")
                .handler(authorizationHandler::superUserPermission);
        router.get("/api/v1/clusters/:clusterName/brokers/leader")
                .handler(authorizationHandler::superUserPermission);
    }

    private void loadPulsarAdminClusterEndpoint(Router router) {
        router.get("/admin/v2/clusters")
                .handler(clusterHandler::list);
        router.get("/admin/v2/clusters/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::get);
        router.put("/admin/v2/clusters/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::create);
        router.post("/admin/v2/clusters/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::update);
        router.delete("/admin/v2/clusters/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::delete);
        router.get("/admin/v2/clusters/:clusterName/failureDomains")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::listFailureDomains);
    }
}
