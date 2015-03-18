package com.anishek;

import com.google.common.util.concurrent.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Threaded {
    private final RunnerFactory factory;
    private long totalNumber;
    private int numberOfThreads;

    public Threaded(long totalNumber, int numberOfThreads, RunnerFactory factory) {
        this.totalNumber = totalNumber;
        this.numberOfThreads = numberOfThreads;
        this.factory = factory;
    }

    public long run() throws Exception {
        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(numberOfThreads,
                        new ThreadFactoryBuilder().setNameFormat("%d").build()
                ));
        List<DefaultCallback> list = new ArrayList<DefaultCallback>();

        long perThread = totalNumber / numberOfThreads;
        for (int i = 0; i < numberOfThreads; i++) {
            ListenableFuture<Long> submit = executorService.submit(factory.create(i * perThread, (i + 1) * perThread));
            DefaultCallback callback = new DefaultCallback();
            list.add(callback);
            Futures.addCallback(submit, callback);
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        long totalTimeTaken = 0;
        for (DefaultCallback callback : list) {
            totalTimeTaken += callback.timeTaken;
        }
        return totalTimeTaken / list.size();
    }

    static class DefaultCallback implements FutureCallback<Long> {

        long timeTaken;

        @Override
        public void onSuccess(Long aLong) {
            timeTaken = aLong;
        }

        @Override
        public void onFailure(Throwable throwable) {
            throw new RuntimeException("Something failed", throwable);
        }
    }
}



