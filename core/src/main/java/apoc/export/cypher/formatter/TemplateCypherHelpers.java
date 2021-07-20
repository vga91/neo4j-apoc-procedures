package apoc.export.cypher.formatter;

import apoc.export.cypher.TemplateCypher;
import apoc.util.Util;
import com.github.jknack.handlebars.Helper;
import com.github.jknack.handlebars.Options;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.export.cypher.MultiStatementCypherSubGraphExporter.countArtificialUniques;
import static apoc.export.cypher.MultiStatementCypherSubGraphExporter.toLabels;
import static apoc.export.cypher.formatter.CypherFormatterUtils.UNIQUE_ID_LABEL;
import static apoc.export.cypher.formatter.CypherFormatterUtils.UNIQUE_ID_PROP;


public enum TemplateCypherHelpers implements Helper<Object> {

    getGroupedNodes {
        @Override
        public Object apply(final Object value, final Options options) throws IOException {
            try(StringWriter writer = new StringWriter()) {
                CypherFormatterUtils.getGroupedNodes(writer, (TemplateCypher) value, options.param(0), options.param(1), options.param(2), asBoolean(options, 3));
                return writer.toString();
            }
        }
    },


    getGroupedRels {
        @Override
        public Object apply(final Object value, final Options options) throws IOException {
            try(StringWriter writer = new StringWriter()) {
                CypherFormatterUtils.getGroupedRels(writer, (TemplateCypher) value, options.param(0), options.param(1), options.param(2), asBoolean(options, 3));
                return writer.toString();
            }
        }
    },


    initGroupedRels {
        @Override
        public Object apply(final Object value, final Options options) throws IOException {
            TemplateCypher templateCypher = (TemplateCypher) value;
            final int relListSize = ((List<Node>) options.param(0)).size();
            templateCypher.incrementRelCount(relListSize);
            return null;
        }
    },

    initGroupedNodes {
        @Override
        public Object apply(final Object value, final Options options) throws IOException {
            TemplateCypher templateCypher = (TemplateCypher) value;
            final int nodeListSize = ((List<Node>) options.param(0)).size();
            templateCypher.incrementNodeCount(nodeListSize);
            return null;
        }
    },

    modIsZero {
        @Override
        public Object apply(final Object value, final Options options) throws IOException {
            return ((int) value) % (int) options.param(0) == 0;
        }
    },

    toLong {
        @Override
        public Object apply(final Object value, final Options options) throws IOException {
            return (long) (int) value;
        }
    },

    forLoop {
        @Override
        public Object apply(final Object value, final Options options) throws IOException {
            Options.Buffer buffer = options.buffer();
            for(long i = 0; i < (long) value; i = i + (long) options.param(0)) {
                buffer.append(options.fn());
            }
            return buffer;
        }
    },

    statementRel {
        @Override
        public Object apply(final Object value, final Options options) {
            final Relationship rel = (Relationship) value;
            
            TemplateCypher templateCypher = options.param(0);
            CypherFormatter cypherFormatter = templateCypher.getExportConfig().getCypherFormat().getFormatter();
            final String cypher = cypherFormatter.statementForRelationship(rel, options.param(1), options.param(2));
            if (Util.isNotNullOrEmpty(cypher)) {
                templateCypher.getReporter().update(0, 1, Iterables.count(rel.getPropertyKeys()));
                return cypher;
            }
            return null;
        }
    },


    statementNode {
        @Override
        public Object apply(final Object value, final Options options) {
            final Node node = (Node) value;

            TemplateCypher templateCypher = options.param(0);
            CypherFormatter cypherFormatter = templateCypher.getExportConfig().getCypherFormat().getFormatter();
            
            templateCypher.incrementArtificialUniques(countArtificialUniques(node));
            String cypher = cypherFormatter.statementForNode(node, options.param(1), options.param(2), options.param(3));

            if (Util.isNotNullOrEmpty(cypher)) {
                templateCypher.getReporter().update(1, 0, Iterables.count(node.getPropertyKeys()));
                return cypher;
            }
            return null;
        }
    },

    formatConstraint {
        @Override
        public Object apply(final Object value, final Options options) {
            final IndexDefinition constraint = (IndexDefinition) value;
            final Iterable<String> props = constraint.getPropertyKeys();
            final String label = Iterables.single(constraint.getLabels()).name();
            return ((CypherFormatter) options.param(0)).statementForConstraint(label, props);
        }
    },

    statementConstraintUnique {
        @Override
        public Object apply(final Object value, final Options options) {
            String statement = ((CypherFormatter) value).statementForConstraint(UNIQUE_ID_LABEL, Collections.singleton(UNIQUE_ID_PROP));
            if (options.param(0, false)) {
                statement = statement.replaceAll("^CREATE", "DROP");
            }
            return statement;
        }
    },

    statementForCleanup {
        @Override
        public Object apply(final Object value, final Options options) {
            return ((CypherFormatter) value).statementForCleanUp(options.param(0));
        }
    },

    statementForNodeFullTextIndex {
        @Override
        public Object apply(final Object value, final Options options) {
            List<Label> labels = toLabels(options.param(1));
            return ((CypherFormatter) value).statementForNodeFullTextIndex(options.param(0), labels, options.param(2));
        }
    },

    statementForIndex {
        @Override
        public Object apply(final Object value, final Options options) {
            String tokenName = ((List<String>) options.param(0)).get(0);
            return ((CypherFormatter) value).statementForIndex(tokenName, options.param(1));
        }
    },

    keysString {
        @Override
        public Object apply(final Object value, final Options options) {
            return StreamSupport.stream(((Iterable<String>) value).spliterator(), false)
                    .map(key -> "node." + CypherFormatterUtils.quote(key))
                    .collect(Collectors.joining(", "));
        }
    };

    private static boolean asBoolean(Options options, int i) {
        return !options.isFalsy(options.param(i));
    }
}
