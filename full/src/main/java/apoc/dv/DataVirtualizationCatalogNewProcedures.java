package apoc.dv;

import apoc.ApocConfig;
import apoc.Extended;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

@Extended
public class DataVirtualizationCatalogNewProcedures {

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Context
    public ApocConfig apocConfig;

    // TODO

}
