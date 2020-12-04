package apoc.periodic;

import apoc.Pools;
import apoc.create.Create;
import apoc.util.Util;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static apoc.util.Util.merge;

public class Periodic {

    public static final Pattern RUNTIME_PATTERN = Pattern.compile("\\bruntime\\s*=", Pattern.CASE_INSENSITIVE);
    public static final Pattern CYPHER_PREFIX_PATTERN = Pattern.compile("\\bcypher\\b", Pattern.CASE_INSENSITIVE);
    public static final String CYPHER_RUNTIME_SLOTTED = "cypher runtime=slotted ";
    final static Pattern LIMIT_PATTERN = Pattern.compile("\\slimit\\s", Pattern.CASE_INSENSITIVE);

    @Context public GraphDatabaseService db;
    @Context public TerminationGuard terminationGuard;
    @Context public Log log;
    @Context public Pools pools;
    @Context public Transaction tx;

    @Procedure
    @Description("apoc.periodic.list - list all jobs")
    public Stream<JobInfo> list() {
        return pools.getJobList().entrySet().stream().map( (e) -> e.getKey().update(e.getValue()));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.commit(statement,params) - runs the given statement in separate transactions until it returns 0")
    public Stream<RundownResult> commit(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String,Object> parameters) throws ExecutionException, InterruptedException {
        validateQuery(statement);
        Map<String,Object> params = parameters == null ? Collections.emptyMap() : parameters;
        long total = 0, executions = 0, updates = 0;
        long start = System.nanoTime();

        if (!LIMIT_PATTERN.matcher(statement).find()) {
            throw new IllegalArgumentException("the statement sent to apoc.periodic.commit must contain a `limit`");
        }

        AtomicInteger batches = new AtomicInteger();
        AtomicInteger failedCommits = new AtomicInteger();
        Map<String,Long> commitErrors = new ConcurrentHashMap<>();
        AtomicInteger failedBatches = new AtomicInteger();
        Map<String,Long> batchErrors = new ConcurrentHashMap<>();
        String periodicId = new Create().uuid();
        if (log.isDebugEnabled()) {
            log.debug("Starting periodic commit from `%s` in separate thread with id: `%s`", statement, periodicId);
        }
        do {
            Map<String, Object> window = Util.map("_count", updates, "_total", total);
            updates = Util.getFuture(pools.getScheduledExecutorService().submit(() -> {
                batches.incrementAndGet();
                try {
                    return executeNumericResultStatement(statement, merge(window, params));
                } catch(Exception e) {
                    failedBatches.incrementAndGet();
                    recordError(batchErrors, e);
                    return 0L;
                }
            }), commitErrors, failedCommits, 0L);
            total += updates;
            if (updates > 0) executions++;
            if (log.isDebugEnabled()) {
                log.debug("Processed in periodic commit with id %s, no %d executions", periodicId, executions);
            }
        } while (updates > 0 && !Util.transactionIsTerminated(terminationGuard));
        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
        if (log.isDebugEnabled()) {
            log.debug("Terminated periodic commit with id %s with %d executions", periodicId, executions);
        }
        return Stream.of(new RundownResult(total,executions, timeTaken, batches.get(),failedBatches.get(),batchErrors, failedCommits.get(), commitErrors, wasTerminated));
    }

    private static void recordError(Map<String, Long> executionErrors, Exception e) {
        String msg = ExceptionUtils.getRootCause(e).getMessage();
        // String msg = ExceptionUtils.getThrowableList(e).stream().map(Throwable::getMessage).collect(Collectors.joining(","))
        executionErrors.compute(msg, (s, i) -> i == null ? 1 : i + 1);
    }

    public static class RundownResult {
        public final long updates;
        public final long executions;
        public final long runtime;
        public final long batches;
        public final long failedBatches;
        public final Map<String, Long> batchErrors;
        public final long failedCommits;
        public final Map<String, Long> commitErrors;
        public final boolean wasTerminated;

        public RundownResult(long total, long executions, long timeTaken, long batches, long failedBatches, Map<String, Long> batchErrors, long failedCommits, Map<String, Long> commitErrors, boolean wasTerminated) {
            this.updates = total;
            this.executions = executions;
            this.runtime = timeTaken;
            this.batches = batches;
            this.failedBatches = failedBatches;
            this.batchErrors = batchErrors;
            this.failedCommits = failedCommits;
            this.commitErrors = commitErrors;
            this.wasTerminated = wasTerminated;
        }
    }

    private long executeNumericResultStatement(@Name("statement") String statement, @Name("params") Map<String, Object> parameters) {
        return db.executeTransactionally(statement, parameters, result -> {
            String column = Iterables.single(result.columns());
            return result.columnAs(column).stream().mapToLong( o -> (long)o).sum();
        });
    }

    @Procedure
    @Description("apoc.periodic.cancel(name) - cancel job with the given name")
    public Stream<JobInfo> cancel(@Name("name") String name) {
        JobInfo info = new JobInfo(name);
        Future future = pools.getJobList().remove(info);
        if (future != null) {
            future.cancel(false);
            return Stream.of(info.update(future));
        }
        return Stream.empty();
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.submit('name',statement) - submit a one-off background statement")
    public Stream<JobInfo> submit(@Name("name") String name, @Name("statement") String statement) {
        validateQuery(statement);
        JobInfo info = submit(name, () -> {
            try {
                db.executeTransactionally(statement);
            } catch(Exception e) {
                log.warn("in background task via submit", e);
                throw new RuntimeException(e);
            }
        }, log);
        return Stream.of(info);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.repeat('name',statement,repeat-rate-in-seconds, config) submit a repeatedly-called background statement. Fourth parameter 'config' is optional and can contain 'params' entry for nested statement.")
    public Stream<JobInfo> repeat(@Name("name") String name, @Name("statement") String statement, @Name("rate") long rate, @Name(value = "config", defaultValue = "{}") Map<String,Object> config ) {
        validateQuery(statement);
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        JobInfo info = schedule(name, () -> {
            db.executeTransactionally(statement, params);
        },0,rate);
        return Stream.of(info);
    }

    private void validateQuery(String statement) {
        Util.validateQuery(db, statement);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.countdown('name',statement,repeat-rate-in-seconds) submit a repeatedly-called background statement until it returns 0")
    public Stream<JobInfo> countdown(@Name("name") String name, @Name("statement") String statement, @Name("rate") long rate) {
        validateQuery(statement);
        JobInfo info = submit(name, new Countdown(name, statement, rate, log), log);
        info.rate = rate;
        return Stream.of(info);
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    public <T> JobInfo submit(String name, Runnable task, Log log) {
        JobInfo info = new JobInfo(name);
        Future<T> future = pools.getJobList().remove(info);
        if (future != null && !future.isDone()) future.cancel(false);

        Runnable wrappingTask = wrapTask(name, task, log);
        Future newFuture = pools.getScheduledExecutorService().submit(wrappingTask);
        pools.getJobList().put(info,newFuture);
        return info;
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    public JobInfo schedule(String name, Runnable task, long delay, long repeat) {
        JobInfo info = new JobInfo(name,delay,repeat);
        Future future = pools.getJobList().remove(info);
        if (future != null && !future.isDone()) future.cancel(false);

        Runnable wrappingTask = wrapTask(name, task, log);
        ScheduledFuture<?> newFuture = pools.getScheduledExecutorService().scheduleWithFixedDelay(wrappingTask, delay, repeat, TimeUnit.SECONDS);
        pools.getJobList().put(info,newFuture);
        return info;
    }

    private static Runnable wrapTask(String name, Runnable task, Log log) {
        return () -> {
            log.debug("Executing task " + name);
            try {
                task.run();
            } catch (Exception e) {
                log.error("Error while executing task " + name + " because of the following exception (the task will be killed):", e);
                throw e;
            }
            log.debug("Executed task " + name);
        };
    }

    /**
     * invoke cypherAction in batched transactions being feeded from cypherIteration running in main thread
     * @param cypherIterate
     * @param cypherAction
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.iterate('statement returning items', 'statement per item', {batchSize:1000,iterateList:true,parallel:false,params:{},concurrency:50,retries:0}) YIELD batches, total - run the second statement for each item returned by the first statement. Returns number of batches and total processed rows")
    public Stream<BatchAndTotalResult> iterate(
            @Name("cypherIterate") String cypherIterate,
            @Name("cypherAction") String cypherAction,
            @Name("config") Map<String,Object> config) {
        validateQuery(cypherIterate);
        long batchSize = Util.toLong(config.getOrDefault("batchSize", 10000));
        int concurrency = Util.toInteger(config.getOrDefault("concurrency", 50));
        boolean parallel = Util.toBoolean(config.getOrDefault("parallel", false));

        BatchMode batchMode = BatchMode.fromConfig(config);

        long retries = Util.toLong(config.getOrDefault("retries", 0)); // todo sleep/delay or push to end of batch to try again or immediate ?
        Map<String,Object> params = (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());
        int failedParams = Util.toInteger(config.getOrDefault("failedParams", -1));
        try (Result result = tx.execute(slottedRuntime(cypherIterate),params)) {
            Pair<String,Boolean> prepared = PeriodicUtils.prepareInnerStatement(cypherAction, batchMode, result.columns(), "_batch");
            String innerStatement = prepared.first();
            boolean iterateList = prepared.other();
            String periodicId = new Create().uuid();
            log.info("Starting periodic iterate from `%s` operation using iteration `%s` in separate thread with id: `%s`", cypherIterate,cypherAction, periodicId);
            return iterateAndExecuteBatchedInSeparateThread((int)batchSize, parallel, iterateList, retries, result,
                    (tx, p) -> Iterators.count(tx.execute(innerStatement, merge(params, p))), concurrency, failedParams, periodicId);
        }
    }

    static String slottedRuntime(String cypherIterate) {
        if (RUNTIME_PATTERN.matcher(cypherIterate).find()) {
            return cypherIterate;
        }
        Matcher matcher = CYPHER_PREFIX_PATTERN.matcher(cypherIterate.substring(0, Math.min(15,cypherIterate.length())));
        return matcher.find() ? CYPHER_PREFIX_PATTERN.matcher(cypherIterate).replaceFirst(CYPHER_RUNTIME_SLOTTED) : CYPHER_RUNTIME_SLOTTED + cypherIterate;
    }

    private Stream<BatchAndTotalResult> iterateAndExecuteBatchedInSeparateThread(int batchsize, boolean parallel, boolean iterateList, long retries,
                  Iterator<Map<String, Object>> iterator, BiConsumer<Transaction, Map<String, Object>> consumer, int concurrency, int failedParams, String periodicId) {

        ExecutorService pool = parallel ? pools.getDefaultExecutorService() : pools.getSingleExecutorService();
        List<Future<Long>> futures = new ArrayList<>(concurrency);
        BatchAndTotalCollector collector = new BatchAndTotalCollector(terminationGuard, failedParams);
        do {
            if (Util.transactionIsTerminated(terminationGuard)) break;
            if (log.isDebugEnabled()) log.debug("Execute, in periodic iterate with id %s, no %d batch size ", periodicId, batchsize);
            List<Map<String,Object>> batch = Util.take(iterator, batchsize);
            final long currentBatchSize = batch.size();
            ExecuteBatch executeBatch =
                    iterateList ?
                            new ListExecuteBatch(terminationGuard, collector, batch, consumer) :
                            new OneByOneExecuteBatch(terminationGuard, collector, batch, consumer);

            futures.add(Util.inTxFuture(log,
                                        pool,
                                        db,
                                        executeBatch,
                                        retries,
                                        retryCount -> collector.incrementRetried(),
                                        onComplete -> {
                                            collector.incrementBatches();
                                            executeBatch.release();
                                        }));
            /*  TODO: not sure if the block below is required
            if (futures.size() > concurrency) {
                while (futures.stream().noneMatch(Future::isDone)) { // none done yet, block for a bit
                    LockSupport.parkNanos(1000);
                }
                Iterator<Future<Long>> it = futures.iterator();
                while (it.hasNext()) {
                    Future<Long> future = it.next();
                    if (future.isDone()) {
                        collector.incrementSuccesses(Util.getFuture(future, collector.getBatchErrors(), collector.getFailedBatches(), 0L));
                        it.remove();
                    }
                }
            }*/
            collector.incrementCount(currentBatchSize);
            if (log.isDebugEnabled()) {
                log.debug("Processed in periodic iterate with id %s, %d iterations of %d total", periodicId, batchsize, collector.getCount());
            }
        } while (iterator.hasNext());

        boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
        ToLongFunction<Future<Long>> toLongFunction = wasTerminated ?
                f -> Util.getFutureOrCancel(f, collector.getBatchErrors(), collector.getFailedBatches(), 0L) :
                f -> Util.getFuture(f, collector.getBatchErrors(), collector.getFailedBatches(), 0L);
        collector.incrementSuccesses(futures.stream().mapToLong(toLongFunction).sum());

        Util.logErrors("Error during iterate.commit:", collector.getBatchErrors(), log);
        Util.logErrors("Error during iterate.execute:", collector.getOperationErrors(), log);
        if (log.isDebugEnabled()) {
            log.debug("Terminated periodic iterate with id %s with %d executions", periodicId, collector.getCount());
        }
        return Stream.of(collector.getResult());
    }

    private static abstract class ExecuteBatch implements Function<Transaction, Long> {

        protected TerminationGuard terminationGuard;
        protected BatchAndTotalCollector collector;
        protected List<Map<String,Object>> batch;
        protected BiConsumer<Transaction, Map<String, Object>> consumer;

        ExecuteBatch(TerminationGuard terminationGuard,
                     BatchAndTotalCollector collector,
                     List<Map<String, Object>> batch,
                     BiConsumer<Transaction, Map<String, Object>> consumer) {
            this.terminationGuard = terminationGuard;
            this.collector = collector;
            this.batch = batch;
            this.consumer = consumer;
        }

        public void release() {
            terminationGuard = null;
            collector = null;
            batch = null;
            consumer = null;
        }
    }

    private static class ListExecuteBatch extends ExecuteBatch {

        ListExecuteBatch(TerminationGuard terminationGuard,
                         BatchAndTotalCollector collector,
                         List<Map<String, Object>> batch,
                         BiConsumer<Transaction, Map<String, Object>> consumer) {
            super(terminationGuard, collector, batch, consumer);
        }

        @Override
        public final Long apply(Transaction txInThread) {
            if (Util.transactionIsTerminated(terminationGuard)) return 0L;
            Map<String, Object> params = Util.map("_count", collector.getCount(), "_batch", batch);
            return executeAndReportErrors(txInThread, consumer, params, batch, batch.size(), null, collector);
        }
    }

    private static class OneByOneExecuteBatch extends ExecuteBatch {

        OneByOneExecuteBatch(TerminationGuard terminationGuard,
                             BatchAndTotalCollector collector,
                             List<Map<String, Object>> batch,
                             BiConsumer<Transaction, Map<String, Object>> consumer) {
            super(terminationGuard, collector, batch, consumer);
        }

        @Override
        public final Long apply(Transaction txInThread) {
            if (Util.transactionIsTerminated(terminationGuard)) return 0L;
            AtomicLong localCount = new AtomicLong(collector.getCount());
            return batch.stream().mapToLong(
                    p -> {
                        if (localCount.get() % 1000 == 0 && Util.transactionIsTerminated(terminationGuard)) {
                            return 0;
                        }
                        Map<String, Object> params = merge(p, Util.map("_count", localCount.get(), "_batch", batch));
                        return executeAndReportErrors(txInThread, consumer, params, batch, 1, localCount, collector);
                    }).sum();
        }
    }

    private static long executeAndReportErrors(Transaction tx, BiConsumer<Transaction, Map<String, Object>> consumer, Map<String, Object> params,
                                        List<Map<String, Object>> batch, int returnValue, AtomicLong localCount, BatchAndTotalCollector collector) {
        try {
            consumer.accept(tx, params);
            if (localCount!=null) {
                localCount.getAndIncrement();
            }
            return returnValue;
        } catch (Exception e) {
            collector.incrementFailedOps(batch.size());
            collector.amendFailedParamsMap(batch);
            recordError(collector.getOperationErrors(), e);
            throw e;
        }
    }

    public static class JobInfo {
        public final String name;
        public long delay;
        public long rate;
        public boolean done;
        public boolean cancelled;

        public JobInfo(String name) {
            this.name = name;
        }

        public JobInfo(String name, long delay, long rate) {
            this.name = name;
            this.delay = delay;
            this.rate = rate;
        }

        public JobInfo update(Future future) {
            this.done = future.isDone();
            this.cancelled = future.isCancelled();
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof JobInfo && name.equals(((JobInfo) o).name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    private class Countdown implements Runnable {
        private final String name;
        private final String statement;
        private final long rate;
        private transient final Log log;

        public Countdown(String name, String statement, long rate, Log log) {
            this.name = name;
            this.statement = statement;
            this.rate = rate;
            this.log = log;
        }

        @Override
        public void run() {
            if (Periodic.this.executeNumericResultStatement(statement, Collections.emptyMap()) > 0) {
                pools.getScheduledExecutorService().schedule(() -> submit(name, this, log), rate, TimeUnit.SECONDS);
            }
        }
    }
}
