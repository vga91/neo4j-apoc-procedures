package apoc.uuid;

import java.util.Map;

import static apoc.uuid.UuidConfig.ADD_TO_SET_LABELS_KEY;
import static apoc.uuid.UuidConfig.UUID_PROPERTY_KEY;

public class UuidInstallInfo extends UuidInfo {
    public Map<String, Object> batchComputationResult;

    UuidInstallInfo(String label, Map<String, Object> properties, Map<String, Object> batchComputationResult) {
        super(label, true, properties);
        this.batchComputationResult = batchComputationResult;
    }

    public static UuidInstallInfo from(String label, Map<String, Object> addToExistingNodesResult, UuidConfig config) {
        Map<String, Object> properties = Map.of(UUID_PROPERTY_KEY, config.getUuidProperty(),
                ADD_TO_SET_LABELS_KEY, config.isAddToSetLabels());
        return new UuidInstallInfo(label, properties, addToExistingNodesResult);
    }


//        public static UuidInstallInfo fromConfig(UuidConfig config) {
//
//        }
}
