package apoc.sequence;

import java.util.Collections;
import java.util.Map;

import static apoc.util.Util.toBoolean;
import static apoc.util.Util.toLong;

public class SequenceConfig {

    private final long initialValue;
    private final boolean createConstraint;
    private final boolean dropConstraint;

    private final String constraintPropertyName;

    public SequenceConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.initialValue = toLong(config.getOrDefault("initialValue", 0L));
        this.createConstraint = toBoolean(config.getOrDefault("createConstraint", true));
        this.dropConstraint = toBoolean(config.getOrDefault("dropConstraint", true));
        this.constraintPropertyName = (String) config.getOrDefault("constraintPropertyName", "id");
    }

    public boolean isDropConstraint() {
        return dropConstraint;
    }

    public String getConstraintPropertyName() {
        return constraintPropertyName;
    }

    public long getInitialValue() {
        return initialValue;
    }

    public boolean isCreateConstraint() {
        return createConstraint;
    }
}
