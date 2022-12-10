package io.github.pulsarkeeper.client.options;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PulsarKeeperOptions {
    @Builder.Default
    private String host = "127.0.0.1";
    @Builder.Default
    private int port = 9023;
}
