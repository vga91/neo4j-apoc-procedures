package apoc.periodic;

import java.util.List;
import java.util.Map;

public class CurrentBatchResult extends BatchResultBase {
    public final long batchNo;
    
    public CurrentBatchResult(long batchNo, long total, long timeTaken, long committedOperations, long failedOperations, long failedBatches, long retries, Map<String, Long> operationErrors, Map<String, Long> batchErrors, boolean wasTerminated, Map<String, List<Map<String, Object>>> failedParams, Map<String, Long> updateStatistics) {
        super(batchNo, total, timeTaken, committedOperations, failedOperations, failedBatches, retries, operationErrors, batchErrors, wasTerminated, failedParams, updateStatistics);
        this.batchNo = batchNo;
    }
}
