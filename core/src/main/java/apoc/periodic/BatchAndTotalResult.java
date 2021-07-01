package apoc.periodic;

import java.util.List;
import java.util.Map;

public class BatchAndTotalResult extends BatchResultBase {
    public final long batches;
    
    public BatchAndTotalResult(long batches, long total, long timeTaken, long committedOperations, long failedOperations, long failedBatches, long retries, Map<String, Long> operationErrors, Map<String, Long> batchErrors, boolean wasTerminated, Map<String, List<Map<String, Object>>> failedParams, Map<String, Long> updateStatistics) {
        super(batches, total, timeTaken, committedOperations, failedOperations, failedBatches, retries, operationErrors, batchErrors, wasTerminated, failedParams, updateStatistics);
        this.batches = batches;
    }

    public LoopingBatchAndTotalResult inLoop(Object loop) {
        return new LoopingBatchAndTotalResult(loop, batches, total);
    }
}
