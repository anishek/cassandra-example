package com.anishek.patten1.threading;

interface Success<T> {
    T value();

    boolean wasSuccessful();
}
