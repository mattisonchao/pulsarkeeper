package io.github.pulsarkeeper.cli.commands;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.github.pulsarkeeper.cli.Args;
import io.github.pulsarkeeper.client.Clusters;
import io.github.pulsarkeeper.common.json.ObjectMapperFactory;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomain;


@Parameters(commandDescription = "pulsar cluster operation")
public class CommandClusters extends CommandBase {

    @Override
    public void exec(String[] args) {
        Args arg = Args.wrap(args);
        JCommander commander = JCommander.newBuilder()
                .programName("pulsarkeeper clusters")
                .addCommand("list", new List())
                .addCommand("get", new Get())
                .addCommand("create", new Create())
                .addCommand("update", new Update())
                .addCommand("delete", new Delete())
                .addCommand("list-failure-domains", new ListFailureDomain())
                .build();
        try {
            commander.parse(args);
        } catch (Throwable ex) {
            commander.usage();
            return;
        }
        Command command = (Command) commander.getCommands()
                .get(commander.getParsedCommand()).getObjects().get(0);
        command.loadCnx(this.cnx);
        arg.shift();
        command.exec(arg.getArgs());
    }


    @Parameters(commandDescription = "List of clusters")
    private static class List extends CommandBase {

        @Override
        public void exec() {
            println(getClient().getClusters().list().join());
        }
    }

    @Parameters(commandDescription = "Get the cluster information")
    private static class Get extends CommandBase {

        @Parameter(names = {"-cn", "--cluster-name"}, description = "cluster name")
        private String clusterName;

        @SneakyThrows
        @Override
        public void exec() {
            Clusters cluster = getClient().getClusters();
            if (clusterName == null) {
                Set<String> clusters = cluster.list().join();
                String selectedCluster = fzf.select(new ArrayList<>(clusters));
                println(cluster.get(selectedCluster).join());
                return;
            }
            println(cluster.get(clusterName).join());
        }
    }

    @Parameters(commandDescription = "Create a cluster")
    private static class Create extends CommandBase {

        @Parameter(names = {"-cn", "--cluster-name"}, description = "cluster name", required = true)
        private String clusterName;

        @Parameter(names = {"-d", "--cluster-data"}, description = "cluster data", required = true)
        private String data;

        @SneakyThrows
        @Override
        public void exec() {
            Clusters clusters = getClient().getClusters();
            ClusterDataImpl clusterData = ObjectMapperFactory.getThreadLocal()
                    .readValue(data, ClusterDataImpl.class);
            println(clusters.create(clusterName, clusterData).join());
        }
    }

    @Parameters(commandDescription = "Update a cluster")
    private static class Update extends CommandBase {

        @Parameter(names = {"-cn", "--cluster-name"}, description = "cluster name")
        private String clusterName;

        @Parameter(names = {"-d", "--cluster-data"}, description = "cluster data", required = true)
        private String data;

        @SneakyThrows
        @Override
        public void exec() {
            Clusters cluster = getClient().getClusters();
            ClusterDataImpl clusterData = ObjectMapperFactory.getThreadLocal()
                    .readValue(data, ClusterDataImpl.class);
            if (clusterName == null) {
                Set<String> clusters = cluster.list().join();
                String selectedCluster = fzf.select(new ArrayList<>(clusters));
                println(cluster.update(selectedCluster, clusterData).join());
                return;
            }
            println(cluster.update(clusterName, clusterData).join());
        }
    }

    @Parameters(commandDescription = "Delete a cluster")
    private static class Delete extends CommandBase {

        @Parameter(names = {"-cn", "--cluster-name"}, description = "cluster name")
        private String clusterName;

        @SneakyThrows
        @Override
        public void exec() {
            Clusters cluster = getClient().getClusters();
            if (clusterName == null) {
                Set<String> clusters = cluster.list().join();
                String selectedCluster = fzf.select(new ArrayList<>(clusters));
                cluster.delete(selectedCluster).join();
                ok();
                return;
            }
            cluster.delete(clusterName).join();
            println("");
            ok();
        }
    }

    @Parameters(commandDescription = "List of failure domain")
    private static class ListFailureDomain extends CommandBase {

        @Parameter(names = {"-cn", "--cluster-name"}, description = "cluster name")
        private String clusterName;

        @SneakyThrows
        @Override
        public void exec() {
            Clusters cluster = getClient().getClusters();
            if (clusterName == null) {
                Set<String> clusters = cluster.list().join();
                String selectedCluster = fzf.select(new ArrayList<>(clusters));
                Map<String, FailureDomain> failureDomains = cluster.listFailureDomains(selectedCluster).join();
                if (failureDomains.isEmpty()) {
                    println("nil");
                } else {
                    println(failureDomains);
                }
                return;
            }
            Map<String, FailureDomain> failureDomains = cluster.listFailureDomains(clusterName).join();
            println(failureDomains);
        }
    }
}
