package com.anishek.threading;

import java.util.List;

interface PerRunEvaluation<T> {
    T eval(List<ReturnValue<T>> list);
}
