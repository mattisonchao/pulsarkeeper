package io.github.pulsarkeeper.client;

import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import javax.annotation.Nonnull;
import lombok.Getter;

public class PulsarKeeper {
    @Getter
    private final Clusters clusters;

    private PulsarKeeper(PulsarKeeperOptions options) {
        Vertx vertx = Vertx.vertx();
        WebClient client = WebClient.create(vertx);
        this.clusters = new Clusters(client, options);
    }

    public static PulsarKeeper create(@Nonnull PulsarKeeperOptions options) {
        return new PulsarKeeper(options);
    }
}
