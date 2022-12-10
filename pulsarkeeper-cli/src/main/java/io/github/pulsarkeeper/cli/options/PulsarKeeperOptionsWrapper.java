package io.github.pulsarkeeper.cli.options;

import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;
import lombok.Data;

@Data
public class PulsarKeeperOptionsWrapper {
    private String name;
    private PulsarKeeperOptions options;

    public static PulsarKeeperOptionsWrapper getDefault() {
        PulsarKeeperOptionsWrapper options = new PulsarKeeperOptionsWrapper();
        options.setName("default");
        PulsarKeeperOptions defaultOptions = PulsarKeeperOptions.builder().build();
        options.setOptions(defaultOptions);
        return options;
    }
}
