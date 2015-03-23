package com.anishek.threading;

import java.util.List;

public class AverageTimeEvaluation implements PerRunEvaluation<Long> {

    @Override
    public Long eval(List<Success<Long>> list) {
        long totalTimeTaken = 0;
        int count = 0;
        for (Success<Long> success : list) {
            if (success.wasSuccessful()) {
                totalTimeTaken += success.value();
                count++;
            }

        }
        return totalTimeTaken / count;
    }
}
