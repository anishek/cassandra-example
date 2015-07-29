package com.anishek.threading;

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

    public <T extends FutureCallback> List run(T instance) throws Exception {
        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(numberOfThreads,
                        new ThreadFactoryBuilder().setNameFormat("%d").build()
                ));
        List<FutureCallback> list = new ArrayList<>();

        long perThread = totalNumber / numberOfThreads;
        for (int i = 0; i < numberOfThreads; i++) {
            ListenableFuture submit = executorService.submit(factory.create(i * perThread, (i + 1) * perThread));
            FutureCallback futureCallback = instance.getClass().newInstance();
            list.add(futureCallback);
            Futures.addCallback(submit, futureCallback);
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        return list;
    }

}



