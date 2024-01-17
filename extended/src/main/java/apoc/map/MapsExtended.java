package apoc.map;

import apoc.Extended;
import apoc.util.Util;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Extended
public class MapsExtended {

    @UserFunction("apoc.map.renameKey")
    @Description("Adds or updates the given entry in the `MAP`.")
    public Map<String, Object> renameKey(@Name("map") Map<String, Object> map, 
                                         @Name("keyFrom") String keyFrom,
                                         @Name("keyTo") String keyTo,
                                         @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return renameKeyRecursively(map, keyFrom, keyTo, config);
    }

    private Map<String, Object> renameKeyRecursively(Map<String, Object> map, String keyFrom, String keyTo, Map<String, Object> config) {

        boolean removeRecursively = Util.toBoolean(config.getOrDefault("recursive", true));
        HashMap<String, Object> mapToUpdate = new HashMap<>(map);
        if (removeRecursively) {
            extracted(map, keyFrom, keyTo, config, mapToUpdate);
        }
        if (mapToUpdate.containsKey(keyFrom)) {
            Object remove = mapToUpdate.remove(keyFrom);
            mapToUpdate.put(keyTo, remove);
        }
        return mapToUpdate;
    }

    private void extracted(Map<String, Object> map, String keyFrom, String keyTo, Map<String, Object> config, HashMap<String, Object> mapToUpdate) {
        // TODO - change with forEach(k,v -> )
        map.forEach((key, value) -> {
            if (value instanceof Map innerMap) {
                Map map1 = renameKeyRecursively(innerMap, keyFrom, keyTo, config);
//                    entry.setValue(map1);
                // TODO --> e.setValue(newList);??
                mapToUpdate.put(key, map1);
            }
            if (value instanceof List subList) {
                List newList = subList.stream()
                        .map(v -> {
                            if (v instanceof Map subMap) {
                                return renameKeyRecursively(subMap, keyFrom, keyTo, config);
                            }
                            return v;
                        }).toList();
                mapToUpdate.put(key, newList);
            }
        });
    }

}
