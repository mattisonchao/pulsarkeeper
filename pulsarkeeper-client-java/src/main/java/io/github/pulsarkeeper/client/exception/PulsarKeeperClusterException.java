package io.github.pulsarkeeper.client.exception;

public class PulsarKeeperClusterException extends PulsarKeeperException {
    public PulsarKeeperClusterException() {
    }

    public PulsarKeeperClusterException(String message) {
        super(message);
    }

    public PulsarKeeperClusterException(String message, Throwable cause) {
        super(message, cause);
    }

    public PulsarKeeperClusterException(Throwable cause) {
        super(cause);
    }

    public PulsarKeeperClusterException(String message, Throwable cause, boolean enableSuppression,
                                        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public static class ClusterNotFoundException extends PulsarKeeperClusterException {
        public ClusterNotFoundException(String clusterName) {
            super(String.format("cluster name %s not found", clusterName));
        }
    }

    public static class IllegalClusterNameOrDataException
            extends PulsarKeeperClusterException {
        public IllegalClusterNameOrDataException(String clusterName, String clusterData) {
            super(String.format("cluster name %s or data %s is illegal", clusterName, clusterData));
        }
    }

    public static class ClusterDuplicatedException extends PulsarKeeperClusterException {
        public ClusterDuplicatedException(String clusterName) {
            super(String.format("cluster name %s already exists.", clusterName));
        }
    }


}
