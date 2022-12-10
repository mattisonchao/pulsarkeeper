package io.github.pulsarkeeper.common.future;

import io.vertx.core.Future;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

public class CompletableFutures {

    public static <T> CompletableFuture<T> from(@Nonnull Future<T> promise) {
        CompletableFuture<T> future = new CompletableFuture<>();
        promise.onSuccess(future::complete)
                .onFailure(future::completeExceptionally);
        return future;
    }

    public static <T> CompletableFuture<T> autoSwitch(@Nonnull Supplier<CompletableFuture<T>> action,
                                                      @Nonnull Set<String> blackList,
                                                      @Nonnull Executor executor) {
        if (blackList.contains(Thread.currentThread().getName())) {
            return action.get().thenApplyAsync(v -> {
                // Switch to another executor to avoid use sensitive thread do task.
                // Use an extra stack frame to protect thread.
                return v;
            }, executor);
        }
        return action.get();
    }

    public static Throwable unwrap(@Nonnull Throwable ex) {
        if (ex instanceof CompletionException) {
            return ex.getCause();
        } else if (ex instanceof ExecutionException) {
            return ex.getCause();
        } else {
            return ex;
        }
    }
}
