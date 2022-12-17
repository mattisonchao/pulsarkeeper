package io.github.pulsarkeeper.client;


import static io.github.pulsarkeeper.common.json.ObjectMapperFactory.getThreadLocal;
import com.fasterxml.jackson.core.type.TypeReference;
import io.github.pulsarkeeper.client.exception.PulsarKeeperClusterException;
import io.github.pulsarkeeper.client.exception.PulsarKeeperException;
import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.client.WebClient;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomain;

public class Clusters {
    private final PulsarKeeperOptions options;
    private final WebClient client;

    public Clusters(WebClient client, PulsarKeeperOptions options) {
        this.client = client;
        this.options = options;
    }


    public CompletableFuture<Set<String>> list() {
        CompletableFuture<Set<String>> future = new CompletableFuture<>();
        client.get(options.getPort(), options.getHost(), "/api/v1/clusters")
                .send()
                .onSuccess(event -> {
                    if (event.statusCode() != HttpResponseStatus.OK.code()) {
                        future.completeExceptionally(new PulsarKeeperException.UnexpectedHttpCodeException(
                                HttpResponseStatus.OK.code(), event.statusCode()));
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

    public CompletableFuture<ClusterData> get(@Nonnull String clusterName) {
        CompletableFuture<ClusterData> future = new CompletableFuture<>();
        client.get(options.getPort(), options.getHost(), "/api/v1/clusters/" + clusterName)
                .send()
                .onSuccess(event -> {
                    switch (event.statusCode()) {
                        case 200:
                            future.complete(event.bodyAsJson(ClusterDataImpl.class));
                            break;
                        case 404:
                            future.completeExceptionally(
                                    new PulsarKeeperClusterException.ClusterNotFoundException(clusterName));
                            break;
                        default:
                            future.completeExceptionally(new PulsarKeeperException.UnexpectedHttpCodeException(
                                    HttpResponseStatus.OK.code(), event.statusCode()));
                    }
                }).onFailure(future::completeExceptionally);
        return future;
    }

    public CompletableFuture<ClusterData> create(@Nonnull String clusterName, @Nonnull ClusterData clusterData) {
        CompletableFuture<ClusterData> future = new CompletableFuture<>();
        client.post(options.getPort(), options.getHost(), "/api/v1/clusters/" + clusterName)
                .sendJson(clusterData)
                .onSuccess(event -> {
                    switch (event.statusCode()) {
                        case 201:
                            future.complete(event.bodyAsJson(ClusterDataImpl.class));
                            break;
                        case 400:
                            future.completeExceptionally(
                                    new PulsarKeeperClusterException.IllegalClusterNameOrDataException(clusterName,
                                            clusterData.toString()));
                            break;
                        case 422:
                            future.completeExceptionally(
                                    new PulsarKeeperClusterException.ClusterDuplicatedException(clusterName));
                            break;
                        case 409:
                            future.completeExceptionally(
                                    new PulsarKeeperException.OperationConflictException("CREATE CLUSTER"));
                            break;
                        default:
                            future.completeExceptionally(new PulsarKeeperException.UnexpectedHttpCodeException(
                                    HttpResponseStatus.OK.code(), event.statusCode()));
                    }
                }).onFailure(future::completeExceptionally);
        return future;
    }

    public CompletableFuture<ClusterData> update(@Nonnull String clusterName, @Nonnull ClusterData clusterData) {
        CompletableFuture<ClusterData> future = new CompletableFuture<>();
        client.patch(options.getPort(), options.getHost(), "/api/v1/clusters/" + clusterName)
                .sendJson(clusterData)
                .onSuccess(event -> {
                    switch (event.statusCode()) {
                        case 200:
                            future.complete(event.bodyAsJson(ClusterDataImpl.class));
                            break;
                        case 400:
                            future.completeExceptionally(
                                    new PulsarKeeperClusterException.IllegalClusterNameOrDataException(clusterName,
                                            clusterData.toString()));
                            break;
                        case 404:
                            future.completeExceptionally(
                                    new PulsarKeeperClusterException.ClusterNotFoundException(clusterName));
                            break;
                        case 409:
                            future.completeExceptionally(
                                    new PulsarKeeperException.OperationConflictException("UPDATE CLUSTER"));
                            break;
                        default:
                            future.completeExceptionally(new PulsarKeeperException.UnexpectedHttpCodeException(
                                    HttpResponseStatus.OK.code(), event.statusCode()));
                    }
                }).onFailure(future::completeExceptionally);
        return future;
    }

    public CompletableFuture<Void> delete(@Nonnull String clusterName) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        client.delete(options.getPort(), options.getHost(), "/api/v1/clusters/" + clusterName)
                .send()
                .onSuccess(event -> {
                    switch (event.statusCode()) {
                        case 204:
                            future.complete(null);
                            break;
                        case 404:
                            future.completeExceptionally(
                                    new PulsarKeeperClusterException.ClusterNotFoundException(clusterName));
                            break;
                        case 409:
                            future.completeExceptionally(
                                    new PulsarKeeperException.OperationConflictException("DELETE CLUSTER"));
                            break;
                        default:
                            future.completeExceptionally(new PulsarKeeperException.UnexpectedHttpCodeException(
                                    HttpResponseStatus.NO_CONTENT.code(), event.statusCode()));
                    }
                }).onFailure(future::completeExceptionally);
        return future;
    }

    public CompletableFuture<Map<String, FailureDomain>> listFailureDomains(@Nonnull String clusterName) {
        CompletableFuture<Map<String, FailureDomain>> future = new CompletableFuture<>();
        client.get(options.getPort(), options.getHost(), "/api/v1/clusters/" + clusterName + "/domains/failure")
                .send()
                .onSuccess(event -> {
                    if (event.statusCode() != HttpResponseStatus.OK.code()) {
                        future.completeExceptionally(new PulsarKeeperException.UnexpectedHttpCodeException(
                                HttpResponseStatus.OK.code(), event.statusCode()));
                    }
                    try {
                        Map<String, FailureDomain> failureDomains = getThreadLocal().readValue(event.bodyAsString(),
                                new TypeReference<>() {
                                });
                        future.complete(failureDomains);
                    } catch (IOException ex) {
                        future.completeExceptionally(ex);
                    }
                }).onFailure(future::completeExceptionally);
        return future;
    }
}
