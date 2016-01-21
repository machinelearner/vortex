package org.vortex.executor;

public class ExecutorException extends RuntimeException {
    public ExecutorException(String message) {
        super(message);
    }

    public ExecutorException(String message, Throwable exception) {
        super(message, exception);
    }
}
