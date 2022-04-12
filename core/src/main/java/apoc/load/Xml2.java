//package apoc.load;
//
//import apoc.ApocConfig;
//import apoc.export.util.CountingInputStream;
//import apoc.generate.config.InvalidConfigException;
//import apoc.result.MapResult;
//import apoc.result.NodeResult;
//import apoc.util.CompressionAlgo;
//import apoc.util.CompressionConfig;
//import apoc.util.FileUtils;
//import apoc.util.Util;
//import org.apache.commons.lang3.BooleanUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.neo4j.graphdb.Label;
//import org.neo4j.graphdb.RelationshipType;
//import org.neo4j.graphdb.Transaction;
//import org.neo4j.logging.Log;
//import org.neo4j.procedure.Context;
//import org.neo4j.procedure.Description;
//import org.neo4j.procedure.Mode;
//import org.neo4j.procedure.Name;
//import org.neo4j.procedure.Procedure;
//import org.w3c.dom.CharacterData;
//import org.w3c.dom.DOMException;
//import org.w3c.dom.Document;
//import org.w3c.dom.Element;
//import org.w3c.dom.NamedNodeMap;
//import org.w3c.dom.Node;
//import org.w3c.dom.NodeList;
//import org.w3c.dom.ProcessingInstruction;
//import org.xml.sax.Attributes;
//import org.xml.sax.EntityResolver;
//import org.xml.sax.InputSource;
//import org.xml.sax.SAXException;
//import org.xml.sax.helpers.DefaultHandler;
//
//import javax.xml.namespace.QName;
//import javax.xml.parsers.DocumentBuilder;
//import javax.xml.parsers.DocumentBuilderFactory;
//import javax.xml.parsers.ParserConfigurationException;
//import javax.xml.parsers.SAXParser;
//import javax.xml.parsers.SAXParserFactory;
//import javax.xml.stream.XMLInputFactory;
//import javax.xml.stream.XMLStreamConstants;
//import javax.xml.stream.XMLStreamException;
//import javax.xml.stream.XMLStreamReader;
//import javax.xml.xpath.XPath;
//import javax.xml.xpath.XPathConstants;
//import javax.xml.xpath.XPathExpression;
//import javax.xml.xpath.XPathExpressionException;
//import javax.xml.xpath.XPathFactory;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.StringReader;
//import java.net.URL;
//import java.util.ArrayDeque;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.Deque;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.LinkedHashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.TreeMap;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//import java.util.stream.Stream;
//
//import static apoc.util.CompressionConfig.COMPRESSION;
//import static apoc.util.FileUtils.getInputStreamFromBinary;
//import static apoc.util.Util.ERROR_BYTES_OR_STRING;
//
//
//public class Xml2 {
//
//    private static final XMLInputFactory FACTORY = XMLInputFactory.newFactory();
//    static {
//        FACTORY.setProperty(XMLInputFactory.IS_COALESCING, true);
//    }
//
//    @Context
//    public ApocConfig apocConfig;
//
//    @Context
//    public Transaction tx;
//
//    @Context
//    public Log log;
//
//    @Procedure
//    @Description("apoc.load.xml('http://example.com/test.xml', 'xPath',config, false) YIELD value as doc CREATE (p:Person) SET p.name = doc.name - load from XML URL (e.g. web-api) to import XML as single nested map with attributes and _type, _text and _childrenx fields.")
//    public Stream<MapResult> xml(@Name("urlOrBinary") Object urlOrBinary, @Name(value = "path", defaultValue = "/") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config, @Name(value = "simple", defaultValue = "false") boolean simpleMode) throws Exception {
//        return xmlXpathToMapResult(urlOrBinary, simpleMode, path ,config);
//    }
//
////    @UserFunction("apoc.xml.parse")
////    @Description("RETURN apoc.xml.parse(<xml string>, <xPath string>, config, false) AS value")
////    public Map<String, Object> parse(@Name("data") String data, @Name(value = "path", defaultValue = "/") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config, @Name(value = "simple", defaultValue = "false") boolean simpleMode) throws Exception {
////        if (config == null) config = Collections.emptyMap();
////        boolean failOnError = (boolean) config.getOrDefault("failOnError", true);
////        return parse(new ByteArrayInputStream(data.getBytes(Charset.forName("UTF-8"))), simpleMode, path, failOnError)
////                .map(mr -> mr.value).findFirst().orElse(null);
////    }
//
//    private Stream<MapResult> xmlXpathToMapResult(@Name("urlOrBinary") Object urlOrBinary, boolean simpleMode, String path, Map<String, Object> config) throws Exception {
//        if (config == null) config = Collections.emptyMap();
//        boolean failOnError = (boolean) config.getOrDefault("failOnError", true);
//        try {
//            Map<String, Object> headers = (Map) config.getOrDefault("headers", Collections.emptyMap());
//            CountingInputStream is = FileUtils.inputStreamFor(urlOrBinary, headers, null, (String) config.getOrDefault(COMPRESSION, CompressionAlgo.NONE.name()));
//            return parse(is, simpleMode, path, failOnError);
//        } catch (Exception e){
//            if(!failOnError)
//                return Stream.of(new MapResult(Collections.emptyMap()));
//            else
//                throw e;
//        }
//    }
//
////    public interface StructuredNode {
////        /**
////         * Returns a given node at the relative path.
////         */
////        StructuredNode queryNode(String xpath) throws XPathExpressionException;
////
////        /**
////         * Returns a list of nodes at the relative path.
////         */
////        List<StructuredNode> queryNodeList(String xpath)
////                throws XPathExpressionException;
////
////        /**
////         * Boilerplate for array handling....
////         */
////        StructuredNode[] queryNodes(String path) throws XPathExpressionException;
////
////        /**
////         * Returns a property at the given part.
////         */
////        String queryString(String path) throws XPathExpressionException;
////
//////        /**
//////         * Queries a {@link Value} which provides various conversions.
//////         */
//////        Value queryValue(String path) throws XPathExpressionException;
////
////        /**
////         * Checks whether a node or non-empty content is reachable via the given
////         * XPath.
////         */
////        boolean isEmpty(String path) throws XPathExpressionException;
////
////        /**
////         * Returns the current node's name.
////         */
////        String getNodeName();
////    }
//    
//    public interface NodeHandler {
//        NodeList process(XMLNodeImpl node) throws XPathExpressionException;
//    }
//
//    public static class XMLNodeImpl/* implements StructuredNode */{
//
//        private Node node;
//        private final XPathFactory XPATH = XPathFactory.newInstance();
//
//        public XMLNodeImpl(Node root) {
//            node = root;
//        }
//
////        @Override
////        public StructuredNode queryNode(String path)
////                throws XPathExpressionException {
////            Node result = (Node) XPATH.newXPath().compile(path)
////                    .evaluate(node, XPathConstants.NODE);
////            if (result == null) {
////                return null;
////            }
////            return new XMLNodeImpl(result);
////        }
//
////        @Override
////        public List<StructuredNode> queryNodeList(String path)
////                throws XPathExpressionException {
////            NodeList result = (NodeList) XPATH.newXPath().compile(path)
////                    .evaluate(node, XPathConstants.NODESET);
////            List<StructuredNode> resultList = new ArrayList<StructuredNode>(
////                    result.getLength());
////            for (int i = 0; i < result.getLength(); i++) {
////                resultList.add(new XMLNodeImpl(result.item(i)));
////            }
////            return resultList;
////        }
//
////        @Override
////        public StructuredNode[] queryNodes(String path)
////                throws XPathExpressionException {
////            List<StructuredNode> nodes = queryNodeList(path);
////            return nodes.toArray(new StructuredNode[nodes.size()]);
////        }
//
//        public NodeList queryNodes(String path) throws XPathExpressionException {
//            return (NodeList) XPATH.newXPath().compile(path)
//                    .evaluate(node, XPathConstants.NODESET);
//        }
//        
////        @Override
//        public String queryString(String path) throws XPathExpressionException {
//            Object result = XPATH.newXPath().compile(path)
//                    .evaluate(node, XPathConstants.NODESET);
//            if (result == null) {
//                return null;
//            }
//            if (result instanceof Node) {
//                String s = ((Node) result).getTextContent();
//                if (s != null) {
//                    return s.trim();
//                }
//                return s;
//            }
//            return result.toString().trim();
//        }
//
////        @Override
////        public boolean isEmpty(String path) throws XPathExpressionException {
////            String result = queryString(path);
////            return result == null || "".equals(result);
////        }
//
////        @Override
////        public String getNodeName() {
////            return node.getNodeName();
////        }
//
////        @Override
////        public String toString() {
////            return getNodeName();
////        }
//    }
//    
//    private static class SAX2DOMHandler {
//
//        private Document document;
//        private Node root;
//        private Node currentNode;
//        private NodeHandler nodeHandler;
//
//        public SAX2DOMHandler(NodeHandler handler, String uri, String name,
//                              Attributes attributes) throws ParserConfigurationException {
//            this.nodeHandler = handler;
//            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
//            DocumentBuilder loader = factory.newDocumentBuilder();
//            document = loader.newDocument();
//            createElement(name, attributes);
//        }
//
//        private boolean nodeUp() {
//            if (isComplete()) {
//                nodeHandler.process(new XMLNodeImpl(root));
//                return true;
//            }
//            currentNode = currentNode.getParentNode();
//            return false;
//        }
//
//        private boolean isComplete() {
//            return currentNode.equals(root);
//        }
//
//        private void createElement(String name, Attributes attributes) {
//            Element element = document.createElement(name);
//            for (int i = 0; i < attributes.getLength(); i++) {
//                String attrName = attributes.getLocalName(i);
//                if (attrName == null || "".equals(attrName)) {
//                    attrName = attributes.getQName(i);
//                }
//                if (attrName != null || !"".equals(attrName)) {
//                    element.setAttribute(attrName, attributes.getValue(i));
//                }
//            }
//            if (currentNode != null) {
//                currentNode.appendChild(element);
//            } else {
//                root = element;
//                document.appendChild(element);
//            }
//            currentNode = element;
//        }
//
//        public Node getRoot() {
//            return root;
//        }
//
//        public void startElement(String uri, String name, Attributes attributes) {
//            createElement(name, attributes);
//        }
//
//        public void processingInstruction(String target, String data) {
//            ProcessingInstruction instruction = document
//                    .createProcessingInstruction(target, data);
//            currentNode.appendChild(instruction);
//        }
//
//        public boolean endElement(String uri, String name) {
//            if (!currentNode.getNodeName().equals(name)) {
//                throw new DOMException(DOMException.SYNTAX_ERR,
//                        "Unexpected end-tag: " + name + " expected: "
//                                + currentNode.getNodeName());
//            }
//            return nodeUp();
//        }
//
//        public void text(String data) {
//            currentNode.appendChild(document.createTextNode(data));
//        }
//
//        public NodeHandler getNodeHandler() {
//            return nodeHandler;
//        }
//    }
//    
//    private static class XMLReader extends DefaultHandler {
//
//        private boolean isTextNode = false;
//
//        private StringBuilder textNode = new StringBuilder();
//
//
//        @Override
//        public void characters(char[] ch, int start, int length)
//                throws SAXException {
//
//            isTextNode = true;
//            textNode.append(ch, start, length);
//        }
//
//
//        @Override
//        public void endDocument() throws SAXException {
//            // Consider iterating over all activeHandler which are not complete
//            // yet and raise an exception.
//            // For now this is simply ignored to make processing more robust.
//        }
//
//
//        @Override
//        public void endElement(String uri, String localName, String name)
//                throws SAXException {
//            // Delegate to active handlers and deletes them if they are finished...
//
//            if (isTextNode) {
//                String data = textNode.toString();
//                for (SAX2DOMHandler handler : activeHandlers) {
//                    handler.text(data);
//                }
//                textNode = new StringBuilder();
//                isTextNode = false;
//            }
//
//            Iterator<SAX2DOMHandler> iter = activeHandlers.iterator();
//            while (iter.hasNext()) {
//                SAX2DOMHandler handler = iter.next();
//                if (handler.endElement(uri, name)) {
//                    iter.remove();
//                }
//            }
//        }
//
//
//        @Override
//        public void processingInstruction(String target, String data)
//                throws SAXException {
//            // Delegate to active handlers...
//            for (SAX2DOMHandler handler : activeHandlers) {
//                handler.processingInstruction(target, data);
//            }
//        }
//
//
//        @Override
//        public void startElement(String uri, String localName, String name,
//                                 Attributes attributes) throws SAXException {
//            // Delegate to active handlers...
//            for (SAX2DOMHandler handler : activeHandlers) {
//                handler.startElement(uri, name, attributes);
//            }
//            // Start a new handler is necessary
//            try {
//                // QName qualifiedName = new QName(uri, localName);
//                NodeHandler handler = handlers.get(name);
//                if (handler != null) {
//                    activeHandlers.add(new SAX2DOMHandler(handler, uri, name,
//                            attributes));
//                }
//            } catch (ParserConfigurationException e) {
//                throw new SAXException(e);
//            }
//        }
//
//
//        private Map<String, NodeHandler> handlers = new TreeMap<String, NodeHandler>();
//
//        private List<SAX2DOMHandler> activeHandlers = new ArrayList<SAX2DOMHandler>();
//
//
//        /**
//         * Registers a new handler for a qualified name of a node. Handlers are
//         * invoked AFTER the complete node was read. Since documents like BMECat
//         * usually don't mix XML-data, namespaces are ignored for now which eases
//         * the processing a lot (especially xpath related tasks). Namespaces however
//         * could be easily added by repalcing String with QName here.
//         */
//
//
//        public void addHandler(String name, NodeHandler handler) {
//
//            handlers.put(name, handler);
//        }
//
//
////        /**
////         * Returns a XMLNode for the given w3c node.
////         */
////        public static StructuredNode convert(Node node) {
////
////            return new XMLNodeImpl(node);
////        }
//
//
//        class UserInterruptException extends RuntimeException {
//
//            private static final long serialVersionUID = -7454219131982518216L;
//        }
//
//
//        /**
//         * Parses the given stream and using the given monitor
//         */
//        public void parse(InputStream stream) throws ParserConfigurationException,
//                SAXException, IOException {
//
//            try {
//                SAXParserFactory factory = SAXParserFactory.newInstance();
//                SAXParser saxParser = factory.newSAXParser();
//                org.xml.sax.XMLReader reader = saxParser.getXMLReader();
//                reader.setEntityResolver(new EntityResolver() {
//
//                    public InputSource resolveEntity(String publicId,
//                                                     String systemId) throws SAXException, IOException {
//
//                        URL url = new URL(systemId);
//                        // Check if file is local
//                        if ("file".equals(url.getProtocol())) {
//                            // Check if file exists
//                            File file = new File(url.getFile());
//                            if (file.exists()) {
//                                return new InputSource(new FileInputStream(file));
//                            }
//                        }
//                        return null;
//                    }
//                });
//                reader.setContentHandler(this);
//                reader.parse(new InputSource(stream));
//            } catch (UserInterruptException e) {
//                /*
//                 * IGNORED - this is used to cancel parsing if the used tried to
//                 * cancel a process.
//                 */
//            } finally {
//                stream.close();
//            }
//        }
//    }
//
//    // todo - stream
//    private Stream<MapResult> parse(InputStream data, boolean simpleMode, String path, boolean failOnError) throws Exception {
//        List<MapResult> result = new ArrayList<>();
//        try {
//            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
//            documentBuilderFactory.setNamespaceAware(true);
//            documentBuilderFactory.setIgnoringElementContentWhitespace(true);
//            documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
//            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
//            documentBuilder.setEntityResolver((publicId, systemId) -> new InputSource(new StringReader("")));
//
//            InputSource source = new InputSource(data);
////            SAXSource saxSrc = new SAXSource(source);
//            XMLReader r = new XMLReader();
//
//            
//            Document doc = documentBuilder.parse(data);
//            XPathFactory xPathFactory = XPathFactory.newInstance();
//
//            XPath xPath = xPathFactory.newXPath();
//
//            path = StringUtils.isEmpty(path) ? "/" : path;
//            XPathExpression xPathExpression = xPath.compile(path);
////            ((XPathFactoryImpl) xpFactory).getConfiguration();
//            String finalPath = path;
//            r.addHandler("node", new NodeHandler() {
////                @Override
//                public NodeList process(XMLNodeImpl node) throws XPathExpressionException {
//                    return node.queryNodes(finalPath);
//                }
//            });
//            
//            r.parse(data);
//            
////            NodeList nodeList = (NodeList) xPathExpression.evaluate(doc, XPathConstants.NODESET);
//
//            for (int i = 0; i < nodeList.getLength(); i++) {
//                final Deque<Map<String, Object>> stack = new LinkedList<>();
//
//                handleNode(stack, nodeList.item(i), simpleMode);
//                for (int index = 0; index < stack.size(); index++) {
//                    result.add(new MapResult(stack.pollFirst()));
//                }
//            }
//        }
//        catch (FileNotFoundException e){
//            if(!failOnError)
//                return Stream.of(new MapResult(Collections.emptyMap()));
//            else
//                throw e;
//        }
//        catch (Exception e){
//            if(!failOnError)
//                return Stream.of(new MapResult(Collections.emptyMap()));
//            else
//                throw e;
//        }
//        return result.stream();
//    }
//
//    private XMLStreamReader getXMLStreamReader(Object urlOrBinary, XmlImportConfig config) throws IOException, XMLStreamException {
//        InputStream inputStream;
//        if (urlOrBinary instanceof String) {
//            String url = (String) urlOrBinary;
//            apocConfig.checkReadAllowed(url);
//            url = FileUtils.changeFileUrlIfImportDirectoryConstrained(url);
//            var sc = Util.openInputStream(url, null, null, null);
//            inputStream = sc.getStream();
//        } else if (urlOrBinary instanceof byte[]) {
//            inputStream = getInputStreamFromBinary((byte[]) urlOrBinary, config.getCompressionAlgo());
//        } else {
//            throw new RuntimeException(ERROR_BYTES_OR_STRING);
//        }
//        if (config.isFilterLeadingWhitespace()) {
//            inputStream = new SkipWhitespaceInputStream(inputStream);
//        }
//        return FACTORY.createXMLStreamReader(inputStream);
//    }
//
//
//    private boolean proceedReader(XMLStreamReader reader) throws XMLStreamException {
//        if (reader.hasNext()) {
//            do {
//                reader.next();
//            } while (reader.isWhiteSpace());
//            return true;
//        } else {
//            return false;
//        }
//    }
//
//    private void handleNode(Deque<Map<String, Object>> stack, Node node, boolean simpleMode) {
//
//        // Handle document node
//        if (node.getNodeType() == Node.DOCUMENT_NODE) {
//            NodeList children = node.getChildNodes();
//            for (int i = 0; i < children.getLength(); i++) {
//                if (children.item(i).getLocalName() != null) {
//                    handleNode(stack, children.item(i), simpleMode);
//                    return;
//                }
//            }
//        }
//
//        Map<String, Object> elementMap = new LinkedHashMap<>();
//        handleTypeAndAttributes(node, elementMap);
//
//        // Set children
//        NodeList children = node.getChildNodes();
//        int count = 0;
//        for (int i = 0; i < children.getLength(); i++) {
//            Node child = children.item(i);
//
//            // This is to deal with text between xml tags for example new line characters
//            if (child.getNodeType() != Node.TEXT_NODE && child.getNodeType() != Node.CDATA_SECTION_NODE) {
//                handleNode(stack, child, simpleMode);
//                count++;
//            } else {
//                // Deal with text nodes
//                handleTextNode(child, elementMap);
//            }
//        }
//
//        if (children.getLength() > 0) {
//            if (!stack.isEmpty()) {
//                List<Object> nodeChildren = new ArrayList<>();
//                for (int i = 0; i < count; i++) {
//                    nodeChildren.add(stack.pollLast());
//                }
//                String key = simpleMode ? "_" + node.getLocalName() : "_children";
//                Collections.reverse(nodeChildren);
//                if (nodeChildren.size() > 0) {
//                    // Before adding the children we need to handle mixed text
//                    Object text = elementMap.get("_text");
//                    if (text instanceof List) {
//                        for (Object element : (List) text) {
//                            nodeChildren.add(element);
//                        }
//                        elementMap.remove("_text");
//                    }
//
//                    elementMap.put(key, nodeChildren);
//                }
//            }
//        }
//
//        if (!elementMap.isEmpty()) {
//            stack.addLast(elementMap);
//        }
//    }
//
//    /**
//     * Collects type and attributes for the node
//     *
//     * @param node
//     * @param elementMap
//     */
//    private void handleTypeAndAttributes(Node node, Map<String, Object> elementMap) {
//        // Set type
//        if (node.getLocalName() != null) {
//            elementMap.put("_type", node.getLocalName());
//        }
//
//        // Set the attributes
//        if (node.getAttributes() != null) {
//            NamedNodeMap attributeMap = node.getAttributes();
//            for (int i = 0; i < attributeMap.getLength(); i++) {
//                Node attribute = attributeMap.item(i);
//                elementMap.put(attribute.getNodeName(), attribute.getNodeValue());
//            }
//        }
//    }
//
//    /**
//     * Handle TEXT nodes and CDATA nodes
//     *
//     * @param node
//     * @param elementMap
//     */
//    private void handleTextNode(Node node, Map<String, Object> elementMap) {
//        Object text = "";
//        int nodeType = node.getNodeType();
//        switch (nodeType) {
//            case Node.TEXT_NODE:
//                text = normalizeText(node.getNodeValue());
//                break;
//            case Node.CDATA_SECTION_NODE:
//                text = normalizeText(((CharacterData) node).getData());
//                break;
//            default:
//                break;
//        }
//
//        // If the text is valid ...
//        if (!StringUtils.isEmpty(text.toString())) {
//            // We check if we have already collected some text previously
//            Object previousText = elementMap.get("_text");
//            if (previousText != null) {
//                // If we just have a "_text" key than we need to collect to a List
//                text = Arrays.asList(previousText.toString(), text);
//            }
//            elementMap.put("_text", text);
//        }
//    }
//
//    /**
//     * Remove trailing whitespaces and new line characters
//     *
//     * @param text
//     * @return
//     */
//    private String normalizeText(String text) {
//        String[] tokens = StringUtils.split(text, "\n");
//        for (int i = 0; i < tokens.length; i++) {
//            tokens[i] = tokens[i].trim();
//        }
//
//        return StringUtils.join(tokens, " ").trim();
//    }
//
//    private boolean collectionIsAllStrings(Object collection) {
//        if (collection instanceof Collection) {
//            return ((Collection<Object>) collection).stream().allMatch(o -> o instanceof String);
//        } else {
//            return false;
//        }
//    }
//
//    private void amendToList(Map<String, Object> map, String key, Object value) {
//        final Object element = map.get(key);
//        if (element == null) {
//            map.put(key, value);
//        } else {
//            if (element instanceof List) {
//                ((List) element).add(value);
//            } else {
//                List<Object> list = new LinkedList<>();
//                list.add(element);
//                list.add(value);
//                map.put(key, list);
//            }
//        }
//    }
//
//    public static class ParentAndChildPair {
//        private org.neo4j.graphdb.Node parent;
//        private org.neo4j.graphdb.Node previousChild=null;
//
//        public ParentAndChildPair(org.neo4j.graphdb.Node parent) {
//            this.parent = parent;
//        }
//
//        public org.neo4j.graphdb.Node getParent() {
//            return parent;
//        }
//
//        public void setParent(org.neo4j.graphdb.Node parent) {
//            this.parent = parent;
//        }
//
//        public org.neo4j.graphdb.Node getPreviousChild() {
//            return previousChild;
//        }
//
//        public void setPreviousChild(org.neo4j.graphdb.Node previousChild) {
//            this.previousChild = previousChild;
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
//            ParentAndChildPair that = (ParentAndChildPair) o;
//            return parent.equals(that.parent);
//        }
//
//        @Override
//        public int hashCode() {
//            return parent.hashCode();
//        }
//    }
//
//    private static class XmlImportConfig extends CompressionConfig {
//
//        private boolean connectCharacters;
//        private Pattern delimiter;
//        private Label label = Label.label("XmlCharacters");
//        private RelationshipType relType = RelationshipType.withName("NE");
//        private Map<String, String> charactersForTag = new HashMap<>();
//        final private boolean filterLeadingWhitespace;
//
//        public XmlImportConfig(Map<String, Object> config) {
//            super(config);
//            if (config == null) {
//                config = Collections.emptyMap();
//            }
//            connectCharacters = BooleanUtils.toBoolean((Boolean) config.get("connectCharacters"));
//            filterLeadingWhitespace = BooleanUtils.toBoolean((Boolean) config.get("filterLeadingWhitespace"));
//
//            String _delimiter = (String) config.get("delimiter");
//            if (_delimiter != null) {
//                connectCharacters = true;
//            }
//            delimiter = Pattern.compile(_delimiter == null ? "\\s" : _delimiter);
//
//            String _label = (String) config.get("label");
//            if (_label != null) {
//                label = Label.label(_label);
//                connectCharacters = true;
//            }
//
//            String _relType = (String) config.get("relType");
//            if (_relType != null) {
//                relType = RelationshipType.withName(_relType);
//                connectCharacters = true;
//            }
//
//            Map<String,String> _charactersForTag = (Map<String, String>) config.get("charactersForTag");
//            if (_charactersForTag !=null) {
//                charactersForTag = _charactersForTag;
//            }
//
//            if (config.containsKey("createNextWordRelationships")) {
//                throw new InvalidConfigException("usage of `createNextWordRelationships` is no longer allowed. Use `{relType:'NEXT_WORD', label:'XmlWord'}` instead.");
//            }
//        }
//
//        public Pattern getDelimiter() {
//            return delimiter;
//        }
//
//        public Label getLabel() {
//            return label;
//        }
//
//        public RelationshipType getRelType() {
//            return relType;
//        }
//
//        public boolean isConnectCharacters() {
//            return connectCharacters;
//        }
//
//        public Map<String, String> getCharactersForTag() {
//            return charactersForTag;
//        }
//
//        public boolean isFilterLeadingWhitespace() {
//            return filterLeadingWhitespace;
//        }
//
//    }
//
//    private static class ImportState {
//        private final Deque<ParentAndChildPair> parents = new ArrayDeque<>();
//        private org.neo4j.graphdb.Node last;
//        private org.neo4j.graphdb.Node lastWord;
//        private int currentCharacterIndex = 0;
//
//        public ImportState(org.neo4j.graphdb.Node initialNode) {
//            this.last = initialNode;
//            this.lastWord = initialNode;
//        }
//
//        public void push(ParentAndChildPair parentAndChildPair) {
//            parents.push(parentAndChildPair);
//        }
//
//        public org.neo4j.graphdb.Node getLastWord() {
//            return lastWord;
//        }
//
//        public void setLastWord(org.neo4j.graphdb.Node lastWord) {
//            this.lastWord = lastWord;
//        }
//
//        public int getCurrentCharacterIndex() {
//            return currentCharacterIndex;
//        }
//
//        public ParentAndChildPair pop() {
//            return parents.pop();
//        }
//
//        public boolean isEmpty() {
//            return parents.isEmpty();
//        }
//
//        public void updateLast(org.neo4j.graphdb.Node thisNode) {
//            ParentAndChildPair parentAndChildPair = parents.peek();
//            final org.neo4j.graphdb.Node parent = parentAndChildPair.getParent();
//            final org.neo4j.graphdb.Node previousChild = parentAndChildPair.getPreviousChild();
//
//            last.createRelationshipTo(thisNode, RelationshipType.withName("NEXT"));
//            thisNode.createRelationshipTo(parent, RelationshipType.withName("IS_CHILD_OF"));
//            if (previousChild ==null) {
//                thisNode.createRelationshipTo(parent, RelationshipType.withName("FIRST_CHILD_OF"));
//            } else {
//                previousChild.createRelationshipTo(thisNode, RelationshipType.withName("NEXT_SIBLING"));
//            }
//            parentAndChildPair.setPreviousChild(thisNode);
//            last = thisNode;
//        }
//
//        public void addCurrentCharacterIndex(int length) {
//            currentCharacterIndex += length;
//        }
//    }
//
//    @Procedure(mode = Mode.WRITE, value = "apoc.xml.import")
//    @Deprecated
//    @Description("Deprecated by apoc.import.xml")
//    public Stream<NodeResult> importToGraphDeprecated(@Name("url") String url, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException, XMLStreamException {
//        return importToGraph(url, config);
//    }
//
//    @Procedure(mode = Mode.WRITE, value = "apoc.import.xml")
//    @Description("apoc.import.xml(file,config) - imports graph from provided file")
//    public Stream<NodeResult> importToGraph(@Name("urlOrBinary") Object urlOrBinary, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException, XMLStreamException {
//        XmlImportConfig importConfig = new XmlImportConfig(config);
//        //TODO: make labels, reltypes and magic properties configurable
//
//        final XMLStreamReader xml = getXMLStreamReader(urlOrBinary, importConfig);
//
//        // stores parents and their most recent child
//        org.neo4j.graphdb.Node root = tx.createNode(Label.label("XmlDocument"));
//        setPropertyIfNotNull(root, "_xmlVersion", xml.getVersion());
//        setPropertyIfNotNull(root, "_xmlEncoding", xml.getEncoding());
//        if (urlOrBinary instanceof String) {
//            root.setProperty("url", urlOrBinary);
//        }
//        ImportState state = new ImportState(root);
//        state.push(new ParentAndChildPair(root));
//
//        while (xml.hasNext()) {
//            xml.next();
//
//            switch (xml.getEventType()) {
//                case XMLStreamConstants.START_DOCUMENT:
//                    // xmlsteamreader starts off by definition at START_DOCUMENT prior to call next() - so ignore this one
//                    break;
//
//                case XMLStreamConstants.PROCESSING_INSTRUCTION:
//                    org.neo4j.graphdb.Node pi = tx.createNode(Label.label("XmlProcessingInstruction"));
//                    pi.setProperty("_piData", xml.getPIData());
//                    pi.setProperty("_piTarget", xml.getPITarget());
//                    state.updateLast(pi);
//                    break;
//
//                case XMLStreamConstants.START_ELEMENT:
//                    final QName qName = xml.getName();
//                    final org.neo4j.graphdb.Node tag = tx.createNode(Label.label("XmlTag"));
//                    tag.setProperty("_name", qName.getLocalPart());
//                    for (int i=0; i<xml.getAttributeCount(); i++) {
//                        tag.setProperty(xml.getAttributeLocalName(i), xml.getAttributeValue(i));
//                    }
//
//                    state.updateLast(tag);
//                    state.push(new ParentAndChildPair(tag));
//                    break;
//
//                case XMLStreamConstants.CHARACTERS:
//                    List<String> words = parseTextIntoPartsAndDelimiters(xml.getText(), importConfig.getDelimiter());
//                    for (String currentWord : words) {
//                        createCharactersNode(currentWord, state, importConfig);
//                    }
//                    break;
//
//                case XMLStreamConstants.END_ELEMENT:
//
//                    String charactersForTag = importConfig.getCharactersForTag().get(xml.getName().getLocalPart());
//                    if (charactersForTag!=null) {
//                        createCharactersNode(charactersForTag, state, importConfig);
//                    }
//                    ParentAndChildPair parent = state.pop();
//                    if (parent.getPreviousChild()!=null) {
//                        parent.getPreviousChild().createRelationshipTo(parent.getParent(), RelationshipType.withName("LAST_CHILD_OF"));
//                    }
//                    break;
//
//                case XMLStreamConstants.END_DOCUMENT:
//                    state.pop();
//                    break;
//
//                case XMLStreamConstants.COMMENT:
//                case XMLStreamConstants.SPACE:
//                    // intentionally do nothing
//                    break;
//                default:
//                    log.warn("xml file contains a {} type structure - ignoring this.", xml.getEventType());
//            }
//
//        }
//        if (!state.isEmpty()) {
//            throw new IllegalStateException("non empty parents, this indicates a bug");
//        }
//        return Stream.of(new NodeResult(root));
//    }
//
//    private void createCharactersNode(String currentWord, ImportState state, XmlImportConfig importConfig) {
//        org.neo4j.graphdb.Node word = tx.createNode(importConfig.getLabel());
//        word.setProperty("text", currentWord);
//        word.setProperty("startIndex", state.getCurrentCharacterIndex());
//        state.addCurrentCharacterIndex(currentWord.length());
//        word.setProperty("endIndex", state.getCurrentCharacterIndex() - 1);
//
//        state.updateLast(word);
//        if (importConfig.isConnectCharacters()) {
//            state.getLastWord().createRelationshipTo(word, importConfig.getRelType());
//            state.setLastWord(word);
//        }
//    }
//
//    List<String> parseTextIntoPartsAndDelimiters(String sourceString, Pattern delimiterPattern) {
//        Matcher matcher = delimiterPattern.matcher(sourceString);
//        ArrayList<String> result = new ArrayList<>();
//
//        int prevEndIndex = 0;
//        int length = sourceString.length();
//        while (matcher.find()) {
//            int start = matcher.start();
//            int end = matcher.end();
//            if (prevEndIndex != start) {
//                result.add(sourceString.substring(prevEndIndex, start));
//            }
//            result.add(sourceString.substring(start, end));
//            prevEndIndex = end;
//        }
//        if (prevEndIndex!=length) {
//            result.add(sourceString.substring(prevEndIndex, length));
//
//        }
//        return result;
//    }
//
//    private void setPropertyIfNotNull(org.neo4j.graphdb.Node root, String propertyKey, Object value) {
//        if (value!=null) {
//            root.setProperty(propertyKey, value);
//        }
//    }
//
//}
