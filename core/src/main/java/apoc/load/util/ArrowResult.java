package apoc.load.util;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static apoc.export.arrow.ImportArrow.getCurrentIndex;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class ArrowResult {
    public long lineNo;
    public List<Object> list;
    public Map<String, Object> map;

    public ArrowResult(Map<String, ValueVector> header, long lineNo, List<String> ignore, EnumSet<Results> results) {
        this.lineNo = lineNo;
        this.map = results.contains(Results.map) ? createMap(header, lineNo, ignore) : emptyMap();
        this.list = results.contains(Results.list) ? createList(header, lineNo, ignore) : emptyList();
    }

    private List<Object> createList(Map<String, ValueVector> header, long lineNo, List<String> ignore) {
        removeIgnored(header, ignore);

        return header.values().stream()
                .map(vector -> getVectorValue((int) lineNo, vector))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private Map<String, Object> createMap(Map<String, ValueVector> header, long lineNo, List<String> ignore) {
        removeIgnored(header, ignore);

        return header.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), getVectorValue((int) lineNo, entry.getValue())))
                .filter(i -> i.other() != null)
                .collect(Collectors.toMap(Pair::first, Pair::other));
    }

    private Object getVectorValue(int lineNo, ValueVector vector) {
        try {
            if (vector instanceof UInt8Vector) {
                return ((UInt8Vector) vector).get(lineNo);
            } else {
                byte[] value = ((VarCharVector) vector).get(lineNo);
                return getCurrentIndex(value);
            }
        } catch (IllegalStateException e) {
            return null;
        }
    }

    private  void removeIgnored(Map<String, ValueVector> header, List<String> ignore) {
        header.entrySet().removeIf(i -> ignore.contains(i.getKey()));
    }
}
