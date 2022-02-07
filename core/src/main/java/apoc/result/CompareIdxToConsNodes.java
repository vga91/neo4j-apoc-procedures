package apoc.result;

import java.util.Map;
import java.util.Set;

public class CompareIdxToConsNodes extends CompareIdxToCons {
    public String label;

    public CompareIdxToConsNodes(String label) {
        super(label);
        this.label = label;
    }

    @Override
    public String getLabelOrType() {
        return label;
    }
}
