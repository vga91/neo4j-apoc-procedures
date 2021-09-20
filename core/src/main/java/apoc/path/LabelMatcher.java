package apoc.path;

import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.*;

import static apoc.path.PropertyMatcher.matchesProperties;

/**
 * A generic label matcher which evaluates whether or not a node has at least one of the labels added on the matcher.
 * String labels can be added on the matcher. The label can optionally be be prefixed with `:`.
 * Also handles compound labels (multiple labels separated by `:`), and a node will be matched if it has all of the labels
 * in a compound label (order does not matter).
 * If the node only has a subset of the compound label, it will only be matched if that subset is in the matcher.
 * For example, a LabelMatcher with only `Person:Manager` will only match on nodes with both :Person and :Manager, not just one or the other.
 * Any other labels on the matched node would not be relevant and would not affect the match.
 * If the LabelMatcher only had `Person:Manager` and `Person:Boss`, then only nodes with both :Person and :Manager, or :Person and :Boss, would match.
 * Some nodes that would not match would be: :Person, :Boss, :Manager, :Boss:Manager, but :Boss:Person:HeadHoncho would match fine.
 * Also accepts a special `*` label, indicating that the matcher will always return a positive match.
 * LabelMatchers hold no context about what a match means, and do not handle labels prefixed with filter symbols (+, -, /, &gt;).
 * Please strip these symbols from the start of each label before adding to the matcher.
 */
public class LabelMatcher {
    private final boolean regexMode;
    private List<Pair<String, String>> labels = new ArrayList<>();
    private List<Pair<List<String>, String>> compoundLabels;

    public LabelMatcher(boolean regexMode) {
        this.regexMode = regexMode;
    }

    public LabelMatcher addLabel(String label, String props) {
        if ("*".equals(label)) {
            labels = Collections.singletonList(Pair.of("*", props));
            return this;
        }

        if (label.charAt(0) == ':') {
            label = label.substring(1);
        }

        String[] elements = label.split(":");
        if (elements.length == 1) {
            labels.add(Pair.of(label, props));
        } else if (elements.length > 1) {
            if (compoundLabels == null) {
                compoundLabels = new ArrayList<>();
            }

            compoundLabels.add(Pair.of(Arrays.asList(elements), props));
        }

        return this;
    }

    public boolean matchesLabels(Node node) {
        if (labels.size() == 1 && labels.get(0).first().equals("*")) {
            return matchesProperties(node, labels.get(0).other(), regexMode);
        }
        
        Set<String> nodeLabels = new HashSet<>();
        node.getLabels().forEach(label -> nodeLabels.add(label.name()));

        for ( Pair<String, String> labelPair : labels ) {
            final String label = labelPair.first();
            final boolean b = regexMode ? nodeLabels.stream().anyMatch(nodeLabel -> nodeLabel.matches(label)) : nodeLabels.contains(label);
            if (b) { // todo - qui
                return matchesProperties(node, labelPair.other(), regexMode);
            }
        }

        if (compoundLabels != null) {
            for (Pair<List<String>, String> compoundLabelPair : compoundLabels) {
                final List<String> compoundLabel = compoundLabelPair.first();
                final boolean b = regexMode ? nodeLabels.stream().anyMatch( nodeLabel -> compoundLabel.stream().anyMatch(nodeLabel::matches) ) : nodeLabels.containsAll(compoundLabel);
                if (b) {  // todo - qui
                    return matchesProperties(node, compoundLabelPair.other(), regexMode); 
                }
            }
        }

        return false;
    }

    public boolean isEmpty() {
        return labels.isEmpty() && (compoundLabels == null || compoundLabels.isEmpty());
    }
}


