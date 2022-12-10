package io.github.pulsarkeeper.server;

import io.github.pulsarkeeper.server.handler.AuthenticationHandler;
import io.github.pulsarkeeper.server.handler.AuthorizationHandler;
import io.github.pulsarkeeper.server.handler.ClusterHandler;
import io.github.pulsarkeeper.server.handler.impl.AuthenticationDisabledHandler;
import io.github.pulsarkeeper.server.handler.impl.AuthorizationDisabledHandler;
import io.github.pulsarkeeper.server.handler.impl.ClusterHandlerImpl;
import io.github.pulsarkeeper.server.options.PulsarKeeperServerOptions;
import io.github.pulsarkeeper.server.resources.ClusterResourcesDelegator;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerService;

@Slf4j
public class PulsarKeeperServer extends AbstractVerticle {
    private final BrokerService brokerService;
    private final PulsarKeeperServerOptions options;
    private ClusterHandler clusterHandler;
    private AuthorizationHandler authorizationHandler;
    private AuthenticationHandler authenticationHandler;

    public PulsarKeeperServer(BrokerService brokerService) {
        this.brokerService = brokerService;
        this.options = new PulsarKeeperServerOptions();
    }

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        PulsarResources pulsarResources = brokerService.getPulsar().getPulsarResources();
        this.clusterHandler = new ClusterHandlerImpl(
                new ClusterResourcesDelegator(pulsarResources.getClusterResources(), vertx.nettyEventLoopGroup())
        );
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
        loadPulsarAdminClusterEndpoint(router);
        server.requestHandler(router)
                .listen(options.getListenerPort(), options.getListenerHost())
                .onSuccess(event -> promise.complete())
                .onFailure(promise::fail);
    }

    private void loadV1ClusterEndpoint(Router router) {
        router.get("/api/v1/cluster")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::list);
        router.get("/api/v1/cluster/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::get);
        router.post("/api/v1/cluster/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::create);
        router.patch("/api/v1/cluster/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::update);
        router.delete("/api/v1/cluster/:name")
                .handler(authorizationHandler::superUserPermission)
                .handler(clusterHandler::delete);
    }

    private void loadPulsarAdminClusterEndpoint(Router router) {
        router.get("/admin/v2/clusters").handler(clusterHandler::list);
        router.get("/admin/v2/clusters/:name").handler(clusterHandler::get);
        router.put("/admin/v2/clusters/:name").handler(clusterHandler::create);
        router.post("/admin/v2/clusters/:name").handler(clusterHandler::update);
        router.delete("/admin/v2/clusters/:name").handler(clusterHandler::delete);
    }
}
