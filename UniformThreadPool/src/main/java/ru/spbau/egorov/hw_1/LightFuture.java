package ru.spbau.egorov.hw_1;


import java.util.function.Function;

/**
 * This interface represents task in thread pool.
 *
 * @param <T> is the type of evaluation result.
 */
public interface LightFuture<T> {
    /**
     * Checks if the evaluation is done.
     *
     * @return true if the task is ready.
     */
    boolean isReady();

    /**
     * Evaluates the value from supplier.
     *
     * @return evaluation result.
     * @throws LightExecutionException if the supplier`s get throws runtime exception.
     */
    T get() throws LightExecutionException;

    /**
     * Apply function to the previous result.
     *
     * @param f is the function to apply to the previous result.
     * @return evaluation result.
     */
    LightFuture<T> thenApply(Function<T, T> f);
}
