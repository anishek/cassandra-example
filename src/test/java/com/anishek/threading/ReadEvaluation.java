package com.anishek.threading;

import com.anishek.ReadRunnable;

import java.util.List;

public class ReadEvaluation implements PerRunEvaluation<ReadRunnable.ReadResult> {
    @Override
    public ReadRunnable.ReadResult eval(List<ReturnValue<ReadRunnable.ReadResult>> list) {
        long timeTaken = 0;
        long rowsRead = 0;
        for (ReturnValue<ReadRunnable.ReadResult> result : list) {
            ReadRunnable.ReadResult value = result.value();
            System.out.println("Average time taken to read " + value.averageRowsRead + " is " + value.timeTaken);
            timeTaken += value.timeTaken;
            rowsRead += value.averageRowsRead;
        }
        ReadRunnable.ReadResult result = new ReadRunnable.ReadResult();
        result.timeTaken = timeTaken / list.size();
        result.averageRowsRead = rowsRead / list.size();
        return result;
    }
}
