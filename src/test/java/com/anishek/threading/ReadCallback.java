package com.anishek.threading;

import com.anishek.ReadResult;
import com.google.common.util.concurrent.FutureCallback;

public class ReadCallback implements FutureCallback<ReadResult>, ReturnValue<ReadResult> {
    private ReadResult result;

    @Override
    public void onSuccess(ReadResult readResult) {
        this.result = readResult;
    }

    @Override
    public void onFailure(Throwable throwable) {
        throw new RuntimeException("Something failed", throwable);
    }

    @Override
    public ReadResult value() {
        return result;
    }
}
