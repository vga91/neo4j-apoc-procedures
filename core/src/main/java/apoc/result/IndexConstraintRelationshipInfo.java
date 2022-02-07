package apoc.result;

import java.util.List;

/**
 * Created by alberto.delazzari on 04/07/17.
 */
public class IndexConstraintRelationshipInfo extends IndexConstraintEntityInfo {

    public final Object type;

    public final String status;

    public IndexConstraintRelationshipInfo(String name, Object type, List<String> properties, String status) {
        super(name, properties);
        this.type = type;
        this.status = status;
    }
}
