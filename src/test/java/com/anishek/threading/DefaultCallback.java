package com.anishek.threading;

import com.google.common.util.concurrent.FutureCallback;

public class DefaultCallback implements FutureCallback<Long>, ReturnValue<Long> {

    long timeTaken;

    @Override
    public void onSuccess(Long aLong) {
        timeTaken = aLong;
    }

    @Override
    public void onFailure(Throwable throwable) {
        throw new RuntimeException("Something failed", throwable);
    }

    @Override
    public Long value() {
        return timeTaken;
    }
}
