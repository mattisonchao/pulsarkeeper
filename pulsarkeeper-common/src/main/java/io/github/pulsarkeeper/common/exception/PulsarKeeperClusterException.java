package io.github.pulsarkeeper.common.exception;

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

    public static class FailureDomainNotFoundException extends PulsarKeeperClusterException {
        public FailureDomainNotFoundException(String clusterName, String domainName) {
            super(String.format("cluster %s failure domain %s not found.", clusterName, domainName));
        }
    }

    public static class FailureDomainConflictException extends PulsarKeeperClusterException {

        public FailureDomainConflictException(String message) {
            super(message);
        }

        public FailureDomainConflictException(String domain, String broker) {
            super(String.format("cluster failure domain %s already has broker %s.", domain, broker));
        }
    }

    public static class LeaderBrokerNotFoundException extends PulsarKeeperClusterException {
        public LeaderBrokerNotFoundException(String clusterName) {
            super(String.format("cluster %s leader broker not found.", clusterName));
        }
    }
}
