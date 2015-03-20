package com.anishek.threading;

import java.util.List;

public class AverageTimeEvaluation implements PerRunEvaluation<Long> {

    @Override
    public Long eval(List<ReturnValue<Long>> list) {
        long totalTimeTaken = 0;
        for (ReturnValue<Long> returnValue : list) {
            totalTimeTaken += returnValue.value();
        }
        long runtime = totalTimeTaken / list.size();
        return runtime;
    }
}
