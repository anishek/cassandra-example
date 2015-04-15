package com.anishek.patten1.threading;

import java.util.List;

interface PerRunEvaluation<T> {
    T eval(List<Success<T>> list);
}
