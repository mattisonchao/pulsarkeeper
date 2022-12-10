package io.github.pulsarkeeper.server;

import io.github.pulsarkeeper.server.handler.ClusterHandler;
import io.github.pulsarkeeper.server.handler.impl.ClusterHandlerImpl;
import io.github.pulsarkeeper.server.options.PulsarKeeperServerOptions;
import io.github.pulsarkeeper.server.resources.ClusterResourcesDelegator;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerService;

@Slf4j
public class PulsarKeeperServer extends AbstractVerticle {
    private final BrokerService brokerService;
    private final PulsarKeeperServerOptions options;
    private ClusterHandler clusterHandler;

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
    }

    @Override
    public void start(Promise<Void> promise) {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        loadV1ClusterEndpoint(router);
        loadPulsarAdminClusterEndpoint(router);
        server.requestHandler(router)
                .listen(options.getListenerPort(), options.getListenerHost())
                .onSuccess(event -> promise.complete())
                .onFailure(promise::fail);
    }

    private void loadV1ClusterEndpoint(Router router) {
        /**
         * @api {get} /api/v1/cluster List of cluster
         * @apiName List of cluster
         * @apiGroup Cluster
         *
         * @apiHeaderExample {json} Request-Headers:
         *                  {
         *                      "Content-Type": "application/json"
         *                      "Accept": "application/json"
         *                  }
         * @apiSuccess {String[]} - List of cluster.
         * @apiSuccessExample {json} Success-Response:
         *     HTTP/1.1 200 OK
         *     [
         *       "cluster1"
         *       "cluster2"
         *     ]
         * @apiPermission none
         * @apiVersion 1.0.0
         */
        router.get("/api/v1/cluster").handler(clusterHandler::list);
    }
    private void loadPulsarAdminClusterEndpoint(Router router) {
        router.get("/admin/v2/clusters").handler(clusterHandler::list);
    }
}
