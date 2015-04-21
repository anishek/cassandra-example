package com.anishek.pattern2;

import com.google.common.util.concurrent.FutureCallback;

public class Callback<T> implements FutureCallback<T> {
    T data;

    @Override
    public void onSuccess(T time) {
        data = time;
    }

    @Override
    public void onFailure(Throwable throwable) {
        throw new RuntimeException("an Exception occurred ", throwable);
    }

}
