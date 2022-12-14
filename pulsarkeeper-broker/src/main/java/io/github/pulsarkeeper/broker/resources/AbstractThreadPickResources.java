package io.github.pulsarkeeper.broker.resources;

import com.google.common.collect.ImmutableSet;
import io.github.pulsarkeeper.common.future.CompletableFutures;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public abstract class AbstractThreadPickResources {
    private static final Set<String> threadBlackList = new ImmutableSet.Builder<String>()
            .add("metadata")
            .build();
    ;
    private final Executor ioExecutor;

    public AbstractThreadPickResources(Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
    }

    protected <T> CompletableFuture<T> pick(Supplier<CompletableFuture<T>> action) {
        return CompletableFutures.autoSwitch(action, threadBlackList, ioExecutor);
    }
}
