package io.github.pulsarkeeper.cli.commands;

import com.beust.jcommander.Parameters;
import io.github.pulsarkeeper.cli.Cli;
import io.github.pulsarkeeper.cli.options.PulsarKeeperCliOptions;
import io.github.pulsarkeeper.cli.options.PulsarKeeperOptionsWrapper;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

@Parameters(commandDescription = "Switch pulsarkeeper context")
public class CommandCtx extends CommandBase {

    @Override
    @SneakyThrows
    public void exec(String[] args) {
        if (args.length == 0) {
            PulsarKeeperCliOptions options = Cli.getConf();
            List<String> selection = options.getContext().stream()
                    .map(PulsarKeeperOptionsWrapper::getName)
                    .collect(Collectors.toList());
            String selectedCnx = fzf.select(selection);
            Cli.switchCtx(selectedCnx);
            println("");
            ok();
            return;
        }
        if (args.length == 1) {
            Cli.switchCtx(args[0]);
            ok();
        }
    }
}
