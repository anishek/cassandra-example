package com.anishek.threading;

import com.anishek.InsertSamePartitionRunnableCatchingException;
import com.google.common.util.concurrent.FutureCallback;

public class DefaultCallback implements FutureCallback<Long>, Success<Long> {

    long timeTaken;
    boolean success = true;

    @Override
    public void onSuccess(Long aLong) {
        timeTaken = aLong;
    }

    @Override
    public void onFailure(Throwable throwable) {
        success = false;
        if (throwable instanceof InsertSamePartitionRunnableCatchingException.TimeoutException) {
            InsertSamePartitionRunnableCatchingException.TimeoutException exception = (InsertSamePartitionRunnableCatchingException.TimeoutException) throwable;
            System.out.println("timeout exception : " + exception.timeoutInMicorSeconds);
        } else {
            throw new RuntimeException("something failed", throwable);
        }
    }

    @Override
    public Long value() {
        return timeTaken;
    }

    @Override
    public boolean wasSuccessful() {
        return success;
    }
}