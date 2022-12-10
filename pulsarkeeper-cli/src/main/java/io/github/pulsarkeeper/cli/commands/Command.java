package io.github.pulsarkeeper.cli.commands;

import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;

public interface Command {

    default void loadCnx(PulsarKeeperOptions loadCnx) {

    }

    default void exec(String[] args) {
        exec();
    }

    default void exec() {
        exec(new String[]{});
    }
}
