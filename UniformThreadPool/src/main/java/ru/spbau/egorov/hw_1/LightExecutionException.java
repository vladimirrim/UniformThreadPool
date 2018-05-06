package ru.spbau.egorov.hw_1;

/**
 * This exception is thrown when the supplier throws runtime exception during evaluation.
 */
public class LightExecutionException extends Exception {
    public LightExecutionException(Throwable cause) {
        super("Error occurred during evaluating task.", cause);
    }
}
