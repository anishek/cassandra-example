package com.anishek.patten1.threading;

import com.anishek.patten1.ReadResult;
import com.google.common.util.concurrent.FutureCallback;

public class ReadCallback implements FutureCallback<ReadResult>, Success<ReadResult> {
    private ReadResult result;
    boolean success = true;

    @Override
    public void onSuccess(ReadResult readResult) {
        this.result = readResult;
    }

    @Override
    public void onFailure(Throwable throwable) {
        success = false;
        throw new RuntimeException("Something failed", throwable);
    }

    @Override
    public ReadResult value() {
        return result;
    }

    @Override
    public boolean wasSuccessful() {
        return success;
    }
}
