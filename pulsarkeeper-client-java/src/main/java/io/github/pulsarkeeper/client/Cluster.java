package io.github.pulsarkeeper.client;


import static io.github.pulsarkeeper.common.json.ObjectMapperFactory.getThreadLocal;
import com.fasterxml.jackson.core.type.TypeReference;
import io.github.pulsarkeeper.client.exception.PulsarKeeperException;
import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.client.WebClient;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class Cluster {
    private final PulsarKeeperOptions options;
    private final WebClient client;

    public Cluster(WebClient client, PulsarKeeperOptions options) {
        this.client = client;
        this.options = options;
    }


    public CompletableFuture<Set<String>> getAll() {
        CompletableFuture<Set<String>> future = new CompletableFuture<>();
        client.get(options.getPort(), options.getHost(), "/api/v1/cluster")
                .send()
                .onSuccess(event -> {
                    if (event.statusCode() != HttpResponseStatus.OK.code()) {
                        throw new PulsarKeeperException.UnexpectedHttpCodeException(
                                HttpResponseStatus.OK.code(), event.statusCode());
                    }
                    try {
                        Set<String> clusters = getThreadLocal().readValue(event.bodyAsString(),
                                new TypeReference<>() {
                                });
                        future.complete(clusters);
                    } catch (IOException ex) {
                        future.completeExceptionally(ex);
                    }
                }).onFailure(future::completeExceptionally);
        return future;
    }
}
