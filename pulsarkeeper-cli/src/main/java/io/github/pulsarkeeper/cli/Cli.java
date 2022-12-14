package io.github.pulsarkeeper.cli;

import static java.lang.System.exit;
import com.beust.jcommander.JCommander;
import com.google.common.base.Throwables;
import io.github.pulsarkeeper.cli.commands.Command;
import io.github.pulsarkeeper.cli.commands.CommandClusters;
import io.github.pulsarkeeper.cli.commands.CommandCtx;
import io.github.pulsarkeeper.cli.options.PulsarKeeperCliOptions;
import io.github.pulsarkeeper.cli.options.PulsarKeeperOptionsWrapper;
import io.github.pulsarkeeper.client.options.PulsarKeeperOptions;
import io.github.pulsarkeeper.common.json.ObjectMapperFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;

public class Cli {
    private static final int ERROR_EXIT = 1;
    private static final int OK_EXIT = 0;
    private static final Path CONF_PATH = Path.of(System.getProperty("user.home"), ".pulsarkeeper", "conf.yaml");
    private static final PulsarKeeperCliOptions DEFAULT_OPTIONS = PulsarKeeperCliOptions.getDefault();

    public static void main(String[] args) throws IOException {
        Args arg = Args.wrap(args);
        JCommander commander = JCommander.newBuilder()
                .programName("pulsarkeeper")
                .addCommand("ctx", new CommandCtx())
                .addCommand("clusters", new CommandClusters())
                .build();
        if (arg.isEmpty()) {
            commander.usage();
            return;
        }
        final PulsarKeeperCliOptions options = getConf();
        List<PulsarKeeperOptionsWrapper> contexts = options.getContext();
        Optional<PulsarKeeperOptionsWrapper> currentCtxOpt =
                contexts.stream().filter(cnx -> cnx.getName().equals(options.getCurrentContextName()))
                        .findFirst();
        // --------- Get current cnx
        final PulsarKeeperOptions currentCtx;
        if (currentCtxOpt.isEmpty()) {
            currentCtx = PulsarKeeperOptionsWrapper.getDefault().getOptions();
        } else {
            currentCtx = currentCtxOpt.get().getOptions();
        }
        commander.parse(arg.getRootCommand());
        Command command = (Command) commander.getCommands().get(commander.getParsedCommand()).getObjects().get(0);
        command.loadCnx(currentCtx);
        arg.shift();
        try {
            command.exec(arg.getArgs());
            exit(OK_EXIT);
        } catch (Throwable ex) {
            if (options.getErrorLevel() == ErrorLevel.debug) {
                String details = Throwables.getStackTraceAsString(ex);
                System.err.println(details);
            } else {
                Throwable rootCause = Throwables.getRootCause(ex);
                System.err.println(rootCause);
            }
            exit(ERROR_EXIT);
        }
    }

    public static PulsarKeeperCliOptions getConf() throws IOException {
        // -------- Parse config
        final PulsarKeeperCliOptions options;
        Path dic = CONF_PATH.getParent();
        if (!Files.exists(dic)) {
            Files.createDirectories(dic);
        }
        if (!Files.exists(CONF_PATH)) {
            Files.createFile(CONF_PATH);
            Files.writeString(CONF_PATH, ObjectMapperFactory.getThreadLocalYaml().writeValueAsString(DEFAULT_OPTIONS));
            options = DEFAULT_OPTIONS;
        } else {
            byte[] bytes = Files.readAllBytes(CONF_PATH);
            options = ObjectMapperFactory.getThreadLocalYaml().readValue(bytes, PulsarKeeperCliOptions.class);
        }
        return options;
    }

    @SneakyThrows
    public static void switchCtx(String ctx) throws IllegalArgumentException {
        if (!Files.exists(CONF_PATH)) {
            throw new IllegalArgumentException("context " + ctx + " not found");
        } else {
            byte[] bytes = Files.readAllBytes(CONF_PATH);
            PulsarKeeperCliOptions cliConf =
                    ObjectMapperFactory.getThreadLocalYaml().readValue(bytes, PulsarKeeperCliOptions.class);
            Optional<PulsarKeeperOptionsWrapper> expectedCtx =
                    cliConf.getContext().stream().filter(ct -> ct.getName().equalsIgnoreCase(ctx))
                            .findAny();
            if (expectedCtx.isEmpty()) {
                throw new IllegalArgumentException("context " + ctx + " not found");
            }
            cliConf.setCurrentContextName(ctx);
            Files.writeString(CONF_PATH, ObjectMapperFactory.getThreadLocalYaml().writeValueAsString(cliConf));
        }
    }

}
