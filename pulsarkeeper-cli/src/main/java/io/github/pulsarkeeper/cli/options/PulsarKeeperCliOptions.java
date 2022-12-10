package io.github.pulsarkeeper.cli.options;

import io.github.pulsarkeeper.cli.ErrorLevel;
import java.util.List;
import lombok.Data;

@Data
public class PulsarKeeperCliOptions {
    private ErrorLevel errorLevel;
    private String currentContextName;
    private List<PulsarKeeperOptionsWrapper> context;


    public static PulsarKeeperCliOptions getDefault() {
        PulsarKeeperCliOptions options = new PulsarKeeperCliOptions();
        options.setErrorLevel(ErrorLevel.normal);
        options.setCurrentContextName("default");
        options.setContext(List.of(PulsarKeeperOptionsWrapper.getDefault()));
        return options;
    }
}
