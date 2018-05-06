package ru.spbau.egorov.uniform_thread_pool;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.Thread.sleep;

/**
 * This class implements task executor which uses several threads for parallel evaluation splitting tasks uniformly.
 *
 * @param <T> is the type of evaluation result.
 */
public class UniformThreadPoolImpl<T> {
    private final ConcurrentLinkedQueue<LightFutureImpl> workQueue = new ConcurrentLinkedQueue<>();
    private final ArrayList<Thread> threads = new ArrayList<>();
    private final PriorityBlockingQueue<Pair> threadTasksCount = new PriorityBlockingQueue<>(4, Comparator.comparingInt(o -> o.taskCount));
    private final ArrayList<ConcurrentLinkedQueue<LightFutureImpl>> threadsWorkQueues = new ArrayList<>();
    private final Thread managerThread;

    /**
     * Creates pool with given amount of threads and one extra thread for managing tasks between them.
     *
     * @param numberOfThreads is the number of threads that are running in pool.
     */
    public UniformThreadPoolImpl(int numberOfThreads, int tasksThreshold) {
        for (int i = 0; i < numberOfThreads; i++) {
            Integer index = i;
            threads.add(new Thread(new Runnable() {
                private final ConcurrentLinkedQueue<LightFutureImpl> workQueue = new ConcurrentLinkedQueue<>();

                {
                    threadsWorkQueues.add(workQueue);
                }

                @Override
                public void run() {
                    while (!Thread.interrupted()) {
                        LightFutureImpl nextTask;
                        nextTask = workQueue.poll();
                        if (nextTask != null) {
                            try {
                                nextTask.value = nextTask.sup.get();
                            } catch (RuntimeException e) {
                                nextTask.exception = new LightExecutionException(e);
                            }
                            nextTask.isReady = true;
                            synchronized (threadTasksCount) {
                                threadTasksCount.remove(new Pair(index, workQueue.size() + 1));
                                threadTasksCount.add(new Pair(index, workQueue.size()));
                                threadTasksCount.notify();
                            }
                        }
                    }
                }
            }));

            threadTasksCount.add(new Pair(i, 0));
            threads.get(i).start();
        }

        managerThread = new Thread(() -> {
            while (!Thread.interrupted()) {
                if (workQueue.peek() != null) {
                    LightFutureImpl task = workQueue.poll();
                    synchronized (threadTasksCount) {
                        while (threadTasksCount.peek().taskCount >= tasksThreshold) {
                            try {
                                threadTasksCount.wait();
                            } catch (InterruptedException ignored) {

                            }
                        }
                        Pair minLoadThread = threadTasksCount.poll();
                        threadTasksCount.add(new Pair(minLoadThread.index, minLoadThread.taskCount + 1));
                        synchronized (threadsWorkQueues.get(minLoadThread.index)) {
                            threadsWorkQueues.get(minLoadThread.index).add(task);
                        }
                    }
                }
            }
        });
        managerThread.start();
    }

    /**
     * Adds task to the the least loaded thread in the thread pool.
     *
     * @param sup is the supplier which do the evaluation.
     * @return LightFuture which represents the task.
     */
    public LightFuture<T> addTask(Supplier<T> sup) {
        LightFutureImpl future = new LightFutureImpl(sup);
        workQueue.add(future);
        return future;

    }

    /**
     * Stop all threads in pool through interrupt.
     */
    public void shutdown() {
        managerThread.interrupt();
        for (Thread thread : threads)
            thread.interrupt();
    }

    private class LightFutureImpl implements LightFuture<T> {
        private volatile boolean isReady;
        private Supplier<T> sup;
        private volatile T value;
        private volatile LightExecutionException exception;

        private LightFutureImpl(@NotNull Supplier<T> sup) {
            this.sup = sup;
        }

        @Override
        public boolean isReady() {
            return isReady;
        }

        @Override
        public T get() throws LightExecutionException {
            while (!isReady) {
                Thread.yield();
            }
            if (exception != null)
                throw exception;
            return value;
        }

        @Override
        public LightFuture<T> thenApply(Function<T, T> fun) {
            return addTask(() -> {
                try {
                    return fun.apply(LightFutureImpl.this.get());
                } catch (LightExecutionException e) {
                    throw new RuntimeException(e.getMessage(), e.getCause());
                }
            });
        }
    }

    private static class Pair {
        private int index;
        private int taskCount;

        private Pair(int index, int taskCount) {
            this.index = index;
            this.taskCount = taskCount;
        }
    }
}
