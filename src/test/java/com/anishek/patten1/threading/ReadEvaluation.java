package com.anishek.patten1.threading;

import com.anishek.patten1.ReadResult;

import java.util.List;

public class ReadEvaluation implements PerRunEvaluation<ReadResult> {
    @Override
    public ReadResult eval(List<Success<ReadResult>> list) {
        long timeTaken = 0;
        long rowsRead = 0;
        long countSuccess = 0;
        for (Success<ReadResult> result : list) {
            if (result.wasSuccessful()) {
                ReadResult value = result.value();
//            System.out.println("Average time taken to read " + value.averageRowsRead + " is " + value.timeTaken);
                timeTaken += value.timeTaken;
                rowsRead += value.averageRowsRead;
                countSuccess++;
            }
        }
        ReadResult result = new ReadResult();
        result.timeTaken = timeTaken / countSuccess;
        result.averageRowsRead = rowsRead / countSuccess;
        return result;
    }
}
