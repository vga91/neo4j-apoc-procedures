//package apoc.sequence;
//
//import org.neo4j.kernel.lifecycle.LifecycleAdapter;
//
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.atomic.AtomicLong;
//
//public class SequenceStorage  extends LifecycleAdapter {
//    protected static ConcurrentMap<String, AtomicLong> STORAGE = new ConcurrentHashMap<>();
//
//    public SequenceStorage() {
//        System.out.println("SequenceStorage.SequenceStorage");
//        System.out.println(STORAGE.keySet());
//    }
//
//    @Override
//    public void init() throws Exception {
//        System.out.println("SequenceStorage.init");
//        super.init();
//    }
//
//    @Override
//    public void start() throws Exception {
//        System.out.println("SequenceStorage.start");
//        super.start();
//    }
//
//    @Override
//    public void stop() throws Exception {
//        System.out.println("SequenceStorage.stop");
//        super.stop();
//    }
//
//    @Override
//    public void shutdown() throws Exception {
//        System.out.println("SequenceStorage.shutdown");
//        super.shutdown();
//    }
//}
