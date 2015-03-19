package com.anishek.threading;

import com.anishek.ReadRunnable;
import com.google.common.util.concurrent.FutureCallback;

public class ReadCallback implements FutureCallback<ReadRunnable.ReadResult>, ReturnValue<ReadRunnable.ReadResult> {
    private ReadRunnable.ReadResult result;

    @Override
    public void onSuccess(ReadRunnable.ReadResult readResult) {
        this.result = readResult;
    }

    @Override
    public void onFailure(Throwable throwable) {
        throw new RuntimeException("Something failed", throwable);
    }

    @Override
    public ReadRunnable.ReadResult value() {
        return result;
    }
}
