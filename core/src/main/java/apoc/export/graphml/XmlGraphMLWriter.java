package apoc.export.graphml;

import apoc.export.util.*;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.export.util.MetaInformation.*;

/**
 * @author mh
 * @since 21.01.14
 */
public class XmlGraphMLWriter {
    
    private final Map<String, Map<Class, String>> totalKeyTypes = new HashMap<>();
    private final Transaction tx;

    public XmlGraphMLWriter(Transaction tx) {
        this.tx = tx;
    }

    public void write(SubGraph graph, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter xmlWriter = xmlOutputFactory.createXMLStreamWriter(writer);
        writeHeader(xmlWriter);
        writeKey(xmlWriter, graph, config);
        writeGraph(xmlWriter);
        for (Node node : graph.getNodes()) {
            int props = writeNode(xmlWriter, node, config);
            reporter.update(1, 0, props);
        }
        for (Relationship rel : graph.getRelationships()) {
            int props = writeRelationship(xmlWriter, rel, config);
            reporter.update(0, 1, props);
        }
        writeFooter(xmlWriter);
        reporter.done();
    }

    private void writeKey(XMLStreamWriter writer, SubGraph ops, ExportConfig config) throws Exception {
        Map<String, Map<Class, String>> nodeKeyTypes = new HashMap<>();
        boolean useTypes = config.useTypes();
        if (ops.getAllLabelsInUse().iterator().hasNext()) {
            putSubMap(nodeKeyTypes, "labels", String.class);
        }
        
        if (config.isSampling()) {
            final Result result = tx.execute("CALL apoc.meta.nodeTypeProperties($conf)", 
                    Map.of("conf", getConfWithIncludeLabels(ops, config)));
            
            nodeKeyTypes.putAll(getPropKeyTypes(useTypes, result));
        } else {
            for (Node node : ops.getNodes()) {
                updateKeyTypesForGraphMl(nodeKeyTypes, node, useTypes);
            }
        }
        ExportFormat format = config.getFormat();
        if (format == ExportFormat.GEPHI) {
            putSubMap(nodeKeyTypes, "TYPE", String.class);
        }
        writeKey(writer, nodeKeyTypes, "node", useTypes);

        if (ops.getAllRelationshipTypesInUse().iterator().hasNext()) {
            putSubMap(totalKeyTypes, "label", String.class);
        }
        if (config.isSampling()) {
            final Result result = tx.execute("CALL apoc.meta.relTypeProperties($conf)",
                    Map.of("conf", getConfWithIncludeRels(ops, config)));
            totalKeyTypes.putAll(getPropKeyTypes(useTypes, result));
        } else {
            for (Relationship rel : ops.getRelationships()) {
                updateKeyTypesForGraphMl(totalKeyTypes, rel, useTypes);
            }
        }
        if (format == ExportFormat.GEPHI) {
            putSubMap(totalKeyTypes, "TYPE", String.class);
        }
        writeKey(writer, totalKeyTypes, "edge", useTypes);
        totalKeyTypes.putAll(nodeKeyTypes);
    }

    private void writeKey(XMLStreamWriter writer, Map<String, Map<Class, String>> keyTypes, String forType, boolean useTypes) throws XMLStreamException {
        for (Map.Entry<String, Map<Class, String>> entry : keyTypes.entrySet()) {
            for (Map.Entry<Class, String> subEntry :entry.getValue().entrySet()) {
               final Class typeClass = subEntry.getKey();
               String type = typeFor(typeClass, GRAPHML_ALLOWED);
               if (type == null) continue;
               writer.writeEmptyElement("key");
               // append uuid suffix if necessary
               writer.writeAttribute("id", entry.getKey() + (useTypes ? subEntry.getValue() : StringUtils.EMPTY));
               writer.writeAttribute("for", forType);
               writer.writeAttribute("attr.name", entry.getKey());
               if (useTypes) {
                   if (typeClass.isArray()) {
                       writer.writeAttribute("attr.type", "string");
                       writer.writeAttribute("attr.list", type);
                   } else {
                       writer.writeAttribute("attr.type", type);
                   }
               }
               newLine(writer);
           }
       }
    }

    private Map<String, Map<Class, String>> getPropKeyTypes(boolean useTypes, Result result) {
        return result.stream()
                .filter(map -> map.get("propertyName") != null)
                .collect(Collectors.toMap(map -> (String) map.get("propertyName"),
                        map -> {
                            final List<String> propertyTypes = ((List<String>) map.get("propertyTypes"));
                            return propertyTypes.stream()
                                    .collect(Collectors.toMap(MetaInformation::getClassAndConvertFromMeta,
                                            propMap -> getPropSuffix(useTypes)));
                        }, (e1, e2) -> {
                            e1.putAll(e2);
                            return e1;
                        }));
    }

    private int writeNode(XMLStreamWriter writer, Node node, ExportConfig config) throws XMLStreamException {
        writer.writeStartElement("node");
        writer.writeAttribute("id", id(node));
        writeLabels(writer, node);
        writeLabelsAsData(writer, node, config);
        int props = writeProps(writer, node);
        endElement(writer);
        return props;
    }

    private String id(Node node) {
        return "n" + node.getId();
    }

    private void writeLabels(XMLStreamWriter writer, Node node) throws XMLStreamException {
        String labelsString = getLabelsString(node);
        if (!labelsString.isEmpty()) writer.writeAttribute("labels", labelsString);
    }

    private void writeLabelsAsData(XMLStreamWriter writer, Node node, ExportConfig config) throws XMLStreamException {
        String labelsString = getLabelsString(node);
        if (labelsString.isEmpty()) return;
        String delimiter = ":";
        if (config.getFormat() == ExportFormat.GEPHI) {
            writeData(writer, "TYPE", delimiter + FormatUtils.joinLabels(node, delimiter));
            writeData(writer, "label", getLabelsStringGephi(config, node));
        } else {
            writeData(writer, "labels", labelsString);
        }
    }

    private int writeRelationship(XMLStreamWriter writer, Relationship rel, ExportConfig config) throws XMLStreamException {
        writer.writeStartElement("edge");
        writer.writeAttribute("id", id(rel));
        writer.writeAttribute("source", id(rel.getStartNode()));
        writer.writeAttribute("target", id(rel.getEndNode()));
        writer.writeAttribute("label", rel.getType().name());
        writeData(writer, "label", rel.getType().name());
        if (config.getFormat() == ExportFormat.GEPHI) {
            writeData(writer, "TYPE", rel.getType().name());
        }
        int props = writeProps(writer, rel);
        endElement(writer);
        return props;
    }

    private String id(Relationship rel) {
        return "e" + rel.getId();
    }

    private void endElement(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeEndElement();
        newLine(writer);
    }

    private int writeProps(XMLStreamWriter writer, Entity node) throws XMLStreamException {
        int count = 0;
        for (String prop : node.getPropertyKeys()) {
            Object value = node.getProperty(prop);
            // join prop with uuid suffix if present
            prop = prop + totalKeyTypes.get(prop).getOrDefault(convertPossiblyToPrimitive(value.getClass()), StringUtils.EMPTY);
            writeData(writer, prop, value);
            count++;
        }
        return count;
    }

    private void writeData(XMLStreamWriter writer, String prop, Object value) throws XMLStreamException {
        writer.writeStartElement("data");
        writer.writeAttribute("key", prop);
        if (value != null) {
            writer.writeCharacters(FormatUtils.toString(value));
        }
        writer.writeEndElement();
    }

    private void writeFooter(XMLStreamWriter writer) throws XMLStreamException {
        endElement(writer);
        endElement(writer);
        writer.writeEndDocument();
    }

    private void writeHeader(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartDocument("UTF-8", "1.0");
        newLine(writer);
        writer.writeStartElement("graphml"); // todo properties
        writer.writeNamespace("xmlns", "http://graphml.graphdrawing.org/xmlns");
        writer.writeAttribute("xmlns", "http://graphml.graphdrawing.org/xmlns", "xsi", "http://www.w3.org/2001/XMLSchema-instance");
        writer.writeAttribute("xsi", "", "schemaLocation", "http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd");
        newLine(writer);
    }

    private void writeGraph(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement("graph");
        writer.writeAttribute("id", "G");
        writer.writeAttribute("edgedefault", "directed");
        newLine(writer);
    }

    private void newLine(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeCharacters(System.getProperty("line.separator"));
    }
}
