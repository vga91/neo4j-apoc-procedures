package apoc.create;

import apoc.get.Get;
import apoc.result.*;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.procedure.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphdb.RelationshipType.withName;

public class Create {

    public static final String[] EMPTY_ARRAY = new String[0];
    
    public static final VirtualEntitiesHandler handler = new VirtualEntitiesHandler();

    @Context
    public Transaction tx;

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.node(['Label'], {key:value,...}) - create node with dynamic labels")
    public Stream<NodeResult> node(@Name("label") List<String> labelNames, @Name("props") Map<String, Object> props) {
        return Stream.of(new NodeResult(setProperties(tx.createNode(Util.labels(labelNames)),props)));
    }


    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.addLabels( [node,id,ids,nodes], ['Label',...]) - adds the given labels to the node or nodes")
    public Stream<NodeResult> addLabels(@Name("nodes") Object nodes, @Name("label") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get(tx).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : labels) {
                node.addLabel(label);
            }
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.setProperty( [node,id,ids,nodes], key, value) - sets the given property on the node(s)")
    public Stream<NodeResult> setProperty(@Name("nodes") Object nodes, @Name("key") String key, @Name("value") Object value) {
        return new Get(tx).nodes(nodes).map((r) -> {
            setProperty(r.node, key,toPropertyValue(value));
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.setRelProperty( [rel,id,ids,rels], key, value) - sets the given property on the relationship(s)")
    public Stream<RelationshipResult> setRelProperty(@Name("relationships") Object rels, @Name("key") String key, @Name("value") Object value) {
        return new Get(tx).rels(rels).map((r) -> {
            setProperty(r.rel,key,toPropertyValue(value));
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.setProperties( [node,id,ids,nodes], [keys], [values]) - sets the given properties on the nodes(s)")
    public Stream<NodeResult> setProperties(@Name("nodes") Object nodes, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return new Get(tx).nodes(nodes).map((r) -> {
            setProperties(r.node, Util.mapFromLists(keys,values));
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.removeProperties( [node,id,ids,nodes], [keys]) - removes the given properties from the nodes(s)")
    public Stream<NodeResult> removeProperties(@Name("nodes") Object nodes, @Name("keys") List<String> keys) {
        return new Get(tx).nodes(nodes).map((r) -> {
            keys.forEach( r.node::removeProperty );
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.setRelProperties( [rel,id,ids,rels], [keys], [values]) - sets the given properties on the relationship(s)")
    public Stream<RelationshipResult> setRelProperties(@Name("rels") Object rels, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return new Get(tx).rels(rels).map((r) -> {
            setProperties(r.rel, Util.mapFromLists(keys,values));
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.removeRelProperties( [rel,id,ids,rels], [keys]) - removes the given properties from the relationship(s)")
    public Stream<RelationshipResult> removeRelProperties(@Name("rels") Object rels, @Name("keys") List<String> keys) {
        return new Get(tx).rels(rels).map((r) -> {
            keys.forEach( r.rel::removeProperty);
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.setLabels( [node,id,ids,nodes], ['Label',...]) - sets the given labels, non matching labels are removed on the node or nodes")
    public Stream<NodeResult> setLabels(@Name("nodes") Object nodes, @Name("label") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get(tx).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : node.getLabels()) {
                if (labelNames.contains(label.name())) continue;
                node.removeLabel(label);
            }
            for (Label label : labels) {
                if (node.hasLabel(label)) continue;
                node.addLabel(label);
            }
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.removeLabels( [node,id,ids,nodes], ['Label',...]) - removes the given labels from the node or nodes")
    public Stream<NodeResult> removeLabels(@Name("nodes") Object nodes, @Name("label") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get(tx).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : labels) {
                node.removeLabel(label);
            }
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.nodes(['Label'], [{key:value,...}]) create multiple nodes with dynamic labels")
    public Stream<NodeResult> nodes(@Name("label") List<String> labelNames, @Name("props") List<Map<String, Object>> props) {
        Label[] labels = Util.labels(labelNames);
        return props.stream().map(p -> new NodeResult(setProperties(tx.createNode(labels), p)));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.create.relationship(person1,'KNOWS',{key:value,...}, person2) create relationship with dynamic rel-type")
    public Stream<RelationshipResult> relationship(@Name("from") Node from,
                                                   @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                   @Name("to") Node to) {
        VirtualRelationship.validateNodes(from, to);
        return Stream.of(new RelationshipResult(setProperties(from.createRelationshipTo(to, withName(relType)), props)));
    }

    @Procedure
    @Description("apoc.create.vNode(['Label'], {key:value,...}) returns a virtual node")
    public Stream<NodeResult> vNode(@Name("label") List<String> labelNames, @Name("props") Map<String, Object> props, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return Stream.of(new NodeResult(vNodeFunction(labelNames, props, config)));
    }

    @UserFunction("apoc.create.vNode")
    @Description("apoc.create.vNode(['Label'], {key:value,...}) returns a virtual node")
    public Node vNodeFunction(@Name("label") List<String> labelNames, @Name(value = "props",defaultValue = "{}") Map<String, Object> props, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        VirtualConfig conf = new VirtualConfig(config);
        if (!conf.isMerge()) {
            return createVirtualNode(labelNames, props);
        } else {
            return handler.getNodes().stream().filter(node -> 
                            Iterables.asSet(Iterables.map(Label::name, node.getLabels())).equals(Set.copyOf(labelNames)) 
                                    && isaBoolean(props, node))
                    .findAny()
                    .map(getVirtualNodeVirtualNodeFunction(conf))
                    .orElseGet(getVirtualNodeSupplier(()-> createVirtualNode(labelNames, props), conf));
        }
    }

    private <T extends Entity> Supplier<T> getVirtualNodeSupplier(Supplier<T> supplier, VirtualConfig conf) {
        return () -> {
            final T node = supplier.get();
            conf.getOnCreate().forEach(node::setProperty);
            return node;
        };
    }

    private <T extends Entity> Function<T, T> getVirtualNodeVirtualNodeFunction(VirtualConfig conf) {
        return node -> {
            conf.getOnMatch().forEach(node::setProperty);
            return node;
        };
    }

    private VirtualNode createVirtualNode(List<String> labelNames, Map<String, Object> props) {
        return new VirtualNode(Util.labels(labelNames), props, handler);
    }

    @UserFunction("apoc.create.virtual.fromNode")
    @Description("apoc.create.virtual.fromNode(node, [propertyNames]) returns a virtual node built from an existing node with only the requested properties")
    public Node virtualFromNodeFunction(@Name("node") Node node, @Name("propertyNames") List<String> propertyNames) {
        return new VirtualNode(node, propertyNames, handler);
    }

    @Procedure
    @Description("apoc.create.vNodes(['Label'], [{key:value,...}]) returns virtual nodes")
    public Stream<NodeResult> vNodes(@Name("label") List<String> labelNames, @Name("props") List<Map<String, Object>> props) {
        Label[] labels = Util.labels(labelNames);
        return props.stream().map(p -> new NodeResult(new VirtualNode(labels, p, handler)));
    }

    @Procedure
    @Description("apoc.create.vRelationship(nodeFrom,'KNOWS',{key:value,...}, nodeTo) returns a virtual relationship")
    public Stream<RelationshipResult> vRelationship(@Name("from") Node from, @Name("relType") String relType, @Name("props") Map<String, Object> props, @Name("to") Node to,
            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return Stream.of(new RelationshipResult(vRelationshipFunction(from, relType, props, to, config)));
    }

    @UserFunction("apoc.create.vRelationship")
    @Description("apoc.create.vRelationship(nodeFrom,'KNOWS',{key:value,...}, nodeTo) returns a virtual relationship")
    public Relationship vRelationshipFunction(@Name("from") Node from, @Name("relType") String relType, @Name("props") Map<String, Object> props, @Name("to") Node to,
                                              @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        VirtualConfig conf = new VirtualConfig(config);
        final RelationshipType type = withName(relType);
        if (!conf.isMerge()) {
            return createVirtualRelationship(from, props, to, type);
        } else {
            return handler.getRels().stream().filter(rel -> rel.getType().equals(type) 
                    && rel.getStartNode().equals(from)
                    && rel.getEndNode().equals(to)
                    && isaBoolean(props, rel))
                    .findAny()
                    .map(getVirtualNodeVirtualNodeFunction(conf))
                    .orElseGet(getVirtualNodeSupplier(() -> createVirtualRelationship(from, props, to, type), conf));
        }
        
    }

    private <T extends Entity> boolean isaBoolean(Map<String, Object> props, T entity) {
        return props.entrySet().stream().allMatch(e -> Objects.deepEquals(e.getValue(), entity.getProperty(e.getKey(), null)));
    }

    private VirtualRelationship createVirtualRelationship(Node from, Map<String, Object> props, Node to, RelationshipType type) {
        return new VirtualRelationship(from, to, type, handler).withProperties(props);
    }

    @Procedure(deprecatedBy = "apoc.create.virtualPath")
    @Deprecated
    @Description("apoc.create.vPattern({_labels:['LabelA'],key:value},'KNOWS',{key:value,...}, {_labels:['LabelB'],key:value}) returns a virtual pattern")
    public Stream<VirtualPathResult> vPattern(@Name("from") Map<String, Object> n,
                                              @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                              @Name("to") Map<String, Object> m) {
        n = new LinkedHashMap<>(n);
        m = new LinkedHashMap<>(m);
        RelationshipType type = withName(relType);
        VirtualNode from = new VirtualNode(Util.labels(n.remove("_labels")), n, handler);
        VirtualNode to = new VirtualNode(Util.labels(m.remove("_labels")), m, handler);
        Relationship rel = createVirtualRelationship(from, props, to, withName(relType));
        return Stream.of(new VirtualPathResult(from, rel, to));
    }

    @Procedure(deprecatedBy = "apoc.create.virtualPath")
    @Deprecated
    @Description("apoc.create.vPatternFull(['LabelA'],{key:value},'KNOWS',{key:value,...},['LabelB'],{key:value}) returns a virtual pattern")
    public Stream<VirtualPathResult> vPatternFull(@Name("labelsN") List<String> labelsN, @Name("n") Map<String, Object> n,
                                                  @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                  @Name("labelsM") List<String> labelsM, @Name("m") Map<String, Object> m) {
        RelationshipType type = withName(relType);
        VirtualNode from = new VirtualNode(Util.labels(labelsN), n);
        VirtualNode to = new VirtualNode(Util.labels(labelsM), m);
        Relationship rel = createVirtualRelationship(from, props, to, type);
        return Stream.of(new VirtualPathResult(from, rel, to));
    }

    @Procedure
    @Description("apoc.create.virtualPath(['LabelA'],{key:value},'KNOWS',{key:value,...},['LabelB'],{key:value}) returns a virtual path of nodes joined by a relationship and the associated properties")
    public Stream<VirtualPathResult> virtualPath(@Name("labelsN") List<String> labelsN, @Name("n") Map<String, Object> n,
                                                  @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                  @Name("labelsM") List<String> labelsM, @Name("m") Map<String, Object> m) {
        RelationshipType type = withName(relType);
        VirtualNode from = new VirtualNode(Util.labels(labelsN), n, handler);
        VirtualNode to = new VirtualNode(Util.labels(labelsM), m, handler);
        Relationship rel = createVirtualRelationship(from, props, to, type);
        return Stream.of(new VirtualPathResult(from, rel, to));
    }

    @Procedure
    @Description("apoc.create.clonePathToVirtual")
    public Stream<PathResult> clonePathToVirtual(@Name("path") Path path) {
        return Stream.of(createVirtualPath(path));
    }

    @Procedure
    @Description("apoc.create.clonePathsToVirtual")
    public Stream<PathResult> clonePathsToVirtual(@Name("paths") List<Path> paths) {
        return paths.stream().map(this::createVirtualPath);
    }

    private PathResult createVirtualPath(Path path) {
        final Iterable<Relationship> relationships = path.relationships();
        final Node first = path.startNode();
        VirtualPath virtualPath = new VirtualPath(new VirtualNode(first, Iterables.asList(first.getPropertyKeys()), handler));
        for (Relationship rel : relationships) {
            VirtualNode start = VirtualNode.from(rel.getStartNode(), handler);
            VirtualNode end = VirtualNode.from(rel.getEndNode(), handler);
            virtualPath.addRel(VirtualRelationship.from(start, end, rel, handler));
        }
        return new PathResult(virtualPath);
    }

    private <T extends Entity> T setProperties(T pc, Map<String, Object> p) {
        if (p == null) return pc;
        for (Map.Entry<String, Object> entry : p.entrySet()) {
            setProperty(pc, entry.getKey(), entry.getValue());
        }
        return pc;
    }

    private <T extends Entity> void setProperty(T pc, String key, Object value) {
        if (value == null) pc.removeProperty(key);
        else pc.setProperty(key, toPropertyValue(value));
    }

    @UserFunction
    @Description("apoc.create.uuid() - creates an UUID")
    public String uuid() {
        return UUID.randomUUID().toString();
    }

    private Object toPropertyValue(Object value) {
        if (value instanceof Iterable) {
            Iterable it = (Iterable) value;
            Object first = Iterables.firstOrNull(it);
            if (first == null) return EMPTY_ARRAY;
            return Iterables.asArray(first.getClass(), it);
        }
        return value;
    }

    @Procedure
    @Description("apoc.create.uuids(count) yield uuid - creates 'count' UUIDs ")
    public Stream<UUIDResult> uuids(@Name("count") long count) {
        return LongStream.range(0, count).mapToObj(UUIDResult::new);
    }

    public static class UUIDResult {
        public final long row;
        public final String uuid;

        public UUIDResult(long row) {
            this.row = row;
            this.uuid = UUID.randomUUID().toString();
            // TODO Long.toHexString(uuid.getMostSignificantBits())+Long.toHexString(uuid.getLeastSignificantBits());
        }
    }

}
