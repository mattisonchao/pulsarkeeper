package io.github.pulsarkeeper.server;

import io.github.pulsarkeeper.common.future.CompletableFutures;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.vertx.core.Vertx;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;


@Slf4j
public class Plugin implements ProtocolHandler {

    private Vertx vertx; // init when start

    @Override
    public String protocolName() {
        return "pulsarkeeper";
    }

    @Override
    public boolean accept(String protocol) {
        return "pulsarkeeper".equalsIgnoreCase(protocol);
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        // ignore
    }

    @Override
    public String getProtocolDataToAdvertise() {
        return "";
    }

    @Override
    public void start(BrokerService service) {
        this.vertx = Vertx.vertx();
        CompletableFutures.from(vertx.deployVerticle(new PulsarKeeperServer(service))).join(); // sync deploy.
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        return Collections.emptyMap();
    }

    @Override
    public void close() {
        if (vertx != null) {
            vertx.close();
        }
    }
}
