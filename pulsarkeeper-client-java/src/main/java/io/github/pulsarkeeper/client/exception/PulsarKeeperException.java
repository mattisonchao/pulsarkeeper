package io.github.pulsarkeeper.client.exception;


public class PulsarKeeperException extends RuntimeException {
    public PulsarKeeperException() {
    }

    public PulsarKeeperException(String message) {
        super(message);
    }

    public PulsarKeeperException(String message, Throwable cause) {
        super(message, cause);
    }

    public PulsarKeeperException(Throwable cause) {
        super(cause);
    }

    public PulsarKeeperException(String message, Throwable cause, boolean enableSuppression,
                                 boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public static class UnexpectedHttpCodeException extends PulsarKeeperException {

        public UnexpectedHttpCodeException(int expected, int actual) {
            super(String.format("Unexpected http response code, expect: %s actual: %s", expected, actual));
        }
    }

    public static class OperationConflictException extends PulsarKeeperException {
        public OperationConflictException(String operation) {
            super(String.format("operation [%s] conflict with other admin.", operation));
        }
    }

}
