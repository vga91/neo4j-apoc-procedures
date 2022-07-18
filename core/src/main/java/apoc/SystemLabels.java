package apoc;

import org.neo4j.graphdb.Label;

public enum SystemLabels implements Label {
    ApocCypherProcedures("custom"),
    ApocCypherProceduresMeta(""), // TODO - esportare anche questo, anche se credo che in realta lo faccia già, nell setLastUpdate !!!!!!!
    Procedure(""),
    Function(""),
    ApocUuid("uuid"),
    ApocTriggerMeta(""), // TODO - esportare anche questo !!!!!!!
    ApocTrigger("trigger"),
    DataVirtualizationCatalog("dv");

    private final String featureName;
    
    SystemLabels(String featureName) {
        this.featureName = featureName;
    }

    public String getFeatureName() {
        return featureName;
    }
}
