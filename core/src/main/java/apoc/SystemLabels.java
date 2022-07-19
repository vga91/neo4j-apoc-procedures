package apoc;

import org.neo4j.graphdb.Label;

public enum SystemLabels implements Label {
    ApocCypherProcedures("custom"),
    ApocCypherProceduresMeta(""),
    Procedure(""),
    Function(""),
    ApocUuid("uuid"),
    ApocTriggerMeta(""),
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
