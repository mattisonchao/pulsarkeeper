package io.github.pulsarkeeper.server.options;

import lombok.Data;

@Data
public class PulsarKeeperServerOptions {
    private String listenerHost = "0.0.0.0";
    private int listenerPort = 9023;
}
