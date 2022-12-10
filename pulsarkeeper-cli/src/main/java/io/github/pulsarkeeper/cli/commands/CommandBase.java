package io.github.pulsarkeeper.cli.commands;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.pulsarkeeper.client.PulsarKeeper;
import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;
import io.github.pulsarkeeper.common.json.ObjectMapperFactory;

public abstract class CommandBase implements Command {

    protected PulsarKeeperOptions cnx;

    @Override
    public void loadCnx(PulsarKeeperOptions loadCnx) {
        this.cnx = loadCnx;
    }

    protected PulsarKeeper getClient() {
        PulsarKeeperOptions option = PulsarKeeperOptions.builder()
                .host(cnx.getHost())
                .port(cnx.getPort())
                .build();
        return PulsarKeeper.create(option);
    }


    protected void println(Object obj) {
        if (obj instanceof String) {
            System.out.println(obj);
        } else {
            try {
                String pretty = ObjectMapperFactory.getThreadLocalYaml().writeValueAsString(obj);
                System.out.println(pretty);
            } catch (JsonProcessingException e) {
                System.out.println(obj);
            }
        }
    }

    protected void ok() {
        println("\u001b[32mOk!");
    }

}
