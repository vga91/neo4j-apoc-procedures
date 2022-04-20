//package apoc.load;
//
//import apoc.result.MapResult;
//import org.apache.commons.lang3.StringUtils;
//import org.w3c.dom.CharacterData;
//import org.w3c.dom.NamedNodeMap;
//import org.w3c.dom.Node;
//import org.w3c.dom.NodeList;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.Deque;
//import java.util.LinkedHashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.Spliterator;
//import java.util.Spliterators;
//import java.util.function.Consumer;
//import java.util.stream.StreamSupport;
//
//public class XmlSpliterator extends Spliterators.AbstractSpliterator<MapResult> {
//
//    private final NodeList nodeList;
//    private int index = 0;
//    private final boolean simpleMode;
//    private final boolean stream;
//
//    public XmlSpliterator(NodeList nodeList, boolean simpleMode, boolean stream) {
//        super(Long.MAX_VALUE, Spliterator.ORDERED);
//        this.nodeList = nodeList;
//        this.simpleMode = simpleMode;
//        this.stream = stream;
//    }
//
//    @Override
//    public synchronized boolean tryAdvance(Consumer<? super MapResult> action) {
//        try {
//            System.out.println("XmlSpliterator.tryAdvance");
//            if (index < nodeList.getLength()) {
//                final Deque<Map<String, Object>> stack = new LinkedList<>();
//
//                handleNode(action, stack, nodeList.item(index), simpleMode);
////                for (int index = 0; index < stack.size(); index++) {
////                    result.add(new MapResult(stack.pollFirst()));
////                }
//                // todo - mettere !stream
//                if (!stream) {
//                    stack.iterator().forEachRemaining(item -> action.accept(new MapResult(item)));
//                }
////                stack.stream().flatMap(item -> new MapResult(item));//.forEachRemaining(item -> action.accept(new MapResult(item)));// MapResult::new);
////                action.accept(new MapResult(stack.pollFirst()));
//                index++;
//                return true;
//            }
//            return false;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//
//    private void handleNode(Consumer<? super MapResult> action, Deque<Map<String, Object>> stack, Node node, boolean simpleMode) {
//
//        // Handle document node
//        if (node.getNodeType() == Node.DOCUMENT_NODE) {
//            NodeList children = node.getChildNodes();
//            for (int i = 0; i < children.getLength(); i++) {
//                if (children.item(i).getLocalName() != null) {
//                    handleNode(action, stack, children.item(i), simpleMode);
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
//                handleNode(action, stack, child, simpleMode);
//                count++;
//            } else {
//                // Deal with text nodes
//                handleTextNode(child, elementMap);
//            }
//        }
//
//        if (children.getLength() > 0) {
//            if (!stack.isEmpty() || stream) {
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
//            if (stream) {
//                action.accept(new MapResult(elementMap));
//            } else {
//                stack.addLast(elementMap);
//            }
//        }
//    }
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
//}
