package apoc.create;

import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class VirtualEntitiesHandler {
    private final Set<VirtualNode> nodes = ConcurrentHashMap.newKeySet();
    private final Set<VirtualRelationship> rels = ConcurrentHashMap.newKeySet();

    public Set<VirtualNode> getNodes() {
        return nodes;
    }

    public Set<VirtualRelationship> getRels() {
        return rels;
    }

    public void addNode(VirtualNode node) {
        this.nodes.add(node);
    }

    public void addRel(VirtualRelationship rel) {
        this.rels.add(rel);
    }
}
