package com.anishek.threading;

interface Success<T> {
    T value();

    boolean wasSuccessful();
}
