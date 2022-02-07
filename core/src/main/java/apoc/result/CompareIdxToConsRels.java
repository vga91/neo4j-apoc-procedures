package apoc.result;

import java.util.Map;
import java.util.Set;

public class CompareIdxToConsRels extends CompareIdxToCons {

    public String type;

    public CompareIdxToConsRels(String type) {
        super(type);
        this.type = type;
    }
    
    @Override
    public String getLabelOrType() {
        return type;
    }
}
