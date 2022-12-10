package io.github.pulsarkeeper.server.checker;

import org.apache.pulsar.common.policies.data.ClusterData;

public class ClusterChecker {
    public static boolean checkClusterName(String clusterName) {
        return true;
    }
    public static boolean checkClusterData(ClusterData clusterData) {
        // todo check parameters
        return true;
    }
}
