package ru.spbau.egorov.uniform_thread_pool;

/**
 * This exception is thrown when the supplier throws runtime exception during evaluation.
 */
public class LightExecutionException extends Exception {
    public LightExecutionException(Throwable cause) {
        super("Error occurred during evaluating task.", cause);
    }
}
