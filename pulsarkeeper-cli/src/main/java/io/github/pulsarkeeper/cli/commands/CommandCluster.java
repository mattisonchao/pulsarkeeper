package io.github.pulsarkeeper.cli.commands;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import io.github.pulsarkeeper.cli.Args;

@Parameters(commandDescription = "pulsar cluster operation")
public class CommandCluster extends CommandBase {

    @Override
    public void exec(String[] args) {
        Args arg = Args.wrap(args);
        JCommander commander = JCommander.newBuilder()
                .programName("pulsarkeeper cluster")
                .addCommand("list", new List())
                .build();
        if (arg.isEmpty()) {
            commander.usage();
            return;
        }
        try {
            commander.parse(args);
        } catch (Throwable ex) {
            commander.usage();
            return;
        }
        Command command = (Command) commander.getCommands()
                .get(commander.getParsedCommand()).getObjects().get(0);
        command.loadCnx(this.cnx);
        command.exec();
    }


    @Parameters(commandDescription = "List of cluster")
    private static class List extends CommandBase {

        @Override
        public void exec() {
            println(getClient().getCluster().getAll().join());
        }
    }
}
