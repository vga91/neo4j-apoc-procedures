package apoc.result;

import java.util.List;

/**
 * Created by alberto.delazzari on 04/07/17.
 */
public class IndexConstraintNodeInfo extends IndexConstraintEntityInfo {

    public final Object label;

    public final String status;

    public final String type;

    public final String failure;

    public final double populationProgress;

    public final long size;

    public final double valuesSelectivity;

    public final String userDescription;

    /**
     * Default constructor
     *
     * @param name
     * @param label
     * @param properties
     * @param status status of the index, if it's a constraint it will be empty
     * @param type if it is an index type will be "INDEX" otherwise it will be the type of constraint
     * @param failure
     * @param populationProgress
     * @param size
     * @param userDescription
     */
    public IndexConstraintNodeInfo(String name, Object label, List<String> properties, String status, String type, String failure, float populationProgress, long size, double valuesSelectivity, String userDescription) {
        super(name, properties);
        this.label = label;
        this.status = status;
        this.type = type;
        this.failure = failure;
        this.populationProgress = populationProgress;
        this.size = size;
        this.valuesSelectivity = valuesSelectivity;
        this.userDescription = userDescription;
    }
}
