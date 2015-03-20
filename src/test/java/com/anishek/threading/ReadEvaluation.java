package com.anishek.threading;

import com.anishek.ReadResult;

import java.util.List;

public class ReadEvaluation implements PerRunEvaluation<ReadResult> {
    @Override
    public ReadResult eval(List<ReturnValue<ReadResult>> list) {
        long timeTaken = 0;
        long rowsRead = 0;
        for (ReturnValue<ReadResult> result : list) {
            ReadResult value = result.value();
//            System.out.println("Average time taken to read " + value.averageRowsRead + " is " + value.timeTaken);
            timeTaken += value.timeTaken;
            rowsRead += value.averageRowsRead;
        }
        ReadResult result = new ReadResult();
        result.timeTaken = timeTaken / list.size();
        result.averageRowsRead = rowsRead / list.size();
        return result;
    }
}
