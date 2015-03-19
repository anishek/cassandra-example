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
        System.out.println("Average time: " + runtime);
        return runtime;
    }
}
