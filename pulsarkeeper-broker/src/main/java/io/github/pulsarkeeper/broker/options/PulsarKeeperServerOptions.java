package io.github.pulsarkeeper.broker.options;

import static io.github.pulsarkeeper.broker.PulsarKeeperServer.SIGNATURE;
import java.lang.reflect.Field;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nonnull;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.logging.log4j.util.Strings;

@Data
public class PulsarKeeperServerOptions {
    private int port = 9023;


    @SneakyThrows
    public static PulsarKeeperServerOptions fromProperties(@Nonnull Properties properties) {
        // ----- parse options
        PulsarKeeperServerOptions options = new PulsarKeeperServerOptions();
        for (Map.Entry<Object, Object> property : properties.entrySet()) {
            String propertyName = property.getKey().toString();
            if (propertyName.startsWith(SIGNATURE)) {
                String noPrefixName = propertyName.replace(SIGNATURE, Strings.EMPTY);
                String formatName = noPrefixName.substring(0, 1)
                        .toLowerCase(Locale.ROOT) + noPrefixName.substring(1);
                Field declaredField = options.getClass().getDeclaredField(formatName);
                declaredField.setAccessible(true);
                declaredField.set(options, property.getValue());
            }
        }
        return options;
    }
}
