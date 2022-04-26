package apoc.load;

import apoc.result.MapResult;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.CharacterData;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class XmlSpliterator extends Spliterators.AbstractSpliterator<MapResult> {

    private static final String ROOT_KEY = "__root";
    private List<Map.Entry<String, NodeList>> nodeListList;
    private final Map<String, NodeList> childNodeListList = new HashMap<>();
    private int index = 0;
    private int currentChildIndex = 0;
    private final boolean simpleMode;
    private final boolean stream;
    private final AtomicInteger id = new AtomicInteger();

    public XmlSpliterator(NodeList nodeListList, boolean simpleMode, boolean stream) {
        super(Long.MAX_VALUE, Spliterator.ORDERED);
        this.nodeListList = Collections.singletonList(new AbstractMap.SimpleEntry<>(ROOT_KEY, nodeListList));
        this.simpleMode = simpleMode;
        this.stream = stream;
    }

    @Override
    public synchronized boolean tryAdvance(Consumer<? super MapResult> action) {
        try {
            if (currentChildIndex == this.nodeListList.size()) {
                return false;
            }
            final Map.Entry<String, NodeList> nodeList = this.nodeListList.get(currentChildIndex);
            if (index < nodeList.getValue().getLength()) {
                final Deque<Map<String, Object>> stack = new LinkedList<>();

                if (stream) {
                    handleStreamNode(action, nodeList.getValue().item(index), nodeList.getKey());
                } else {
                    handleNode(stack, nodeList.getValue().item(index), simpleMode);
                }
                
                if (!stream) {
                    stack.iterator().forEachRemaining(item -> action.accept(new MapResult(item)));
                    index++;
                }
                return mapEmptyAndReturnTrue(action);
            }

            if (stream) {
                currentChildIndex++;
                if (nodeListList.size() > currentChildIndex) {
                    index = 0;
                    return mapEmptyAndReturnTrue(action);
                }
                if (childNodeListList.isEmpty()) {
                    return false;
                }
                nodeListList = List.copyOf(childNodeListList.entrySet());
                childNodeListList.clear();
                index = 0;
                currentChildIndex = 0;
                return mapEmptyAndReturnTrue(action);
            }
            return false;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean mapEmptyAndReturnTrue(Consumer<? super MapResult> action) {
        action.accept(MapResult.EMPTY);
        return true;
    }

    private synchronized void handleStreamNode(Consumer<? super MapResult> action, Node node, String parentKey) {
        Map<String, Object> elementMap = new LinkedHashMap<>();
        handleTypeAndAttributes(node, elementMap);

        NodeList children = node.getChildNodes();
        
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);

            // Deal with text nodes
            if (child.getNodeType() == Node.TEXT_NODE || child.getNodeType() == Node.CDATA_SECTION_NODE) {
                handleTextNode(child, elementMap);
            }
        }

        // we assign a _parent_key and a _child_key instead of tree structure in not-stream mode {key,val,key2,val2,_children: [{map1}, {map2}]]...}
        // note that with default non-stream mode we have to handle text differently 
        // e.g a tag like <text>text0<mixed/> text1</text> will be handled in non-stream mode as [{_type=mixed}, text0, text1], _type=text},
        // instead with stream mode we will have a row {_parent_key=XX, _child_key: 'root_1', _type: 'text', _text:['text0', 'text1']} 
        // and another row for the mixed tag {_parent_key: XX, _child_key: 'text_3', _type: 'mixed'}, because we don't have the "_children" key
        final boolean empty = elementMap.isEmpty();
        String key = node.getNodeName() + "_" + id.getAndIncrement();
        if (!empty) {
            elementMap.put("_parent_key", key);
            elementMap.put("_child_key", parentKey);
        }
        if (children.getLength() > 0) {
            childNodeListList.put(key, children);
        }
        
        index++;
        action.accept(empty ? MapResult.EMPTY : new MapResult(elementMap));
    }

    private void handleNode(Deque<Map<String, Object>> stack, Node node, boolean simpleMode) {

        // Handle document node
        if (node.getNodeType() == Node.DOCUMENT_NODE) {
            NodeList children = node.getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                if (children.item(i).getLocalName() != null) {
                    handleNode(stack, children.item(i), simpleMode);
                    return;
                }
            }
        }

        Map<String, Object> elementMap = new LinkedHashMap<>();
        handleTypeAndAttributes(node, elementMap);

        // Set children
        NodeList children = node.getChildNodes();
        int count = 0;
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);

            // This is to deal with text between xml tags for example new line characters
            if (child.getNodeType() != Node.TEXT_NODE && child.getNodeType() != Node.CDATA_SECTION_NODE) {
                handleNode(stack, child, simpleMode);
                count++;
            } else {
                // Deal with text nodes
                handleTextNode(child, elementMap);
            }
        }

        if (children.getLength() > 0) {
            if (!stack.isEmpty()) {
                List<Object> nodeChildren = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    nodeChildren.add(stack.pollLast());
                }
                String key = simpleMode ? "_" + node.getLocalName() : "_children";
                Collections.reverse(nodeChildren);
                if (nodeChildren.size() > 0) {
                    // Before adding the children we need to handle mixed text
                    Object text = elementMap.get("_text");
                    if (text instanceof List) {
                        nodeChildren.addAll((List) text);
                        elementMap.remove("_text");
                    }

                    elementMap.put(key, nodeChildren);
                }
            }
        }

        if (!elementMap.isEmpty()) {
            stack.addLast(elementMap);
        }
    }

    /**
     * Collects type and attributes for the node
     *
     * @param node
     * @param elementMap
     */
    private void handleTypeAndAttributes(Node node, Map<String, Object> elementMap) {
        // Set type
        if (node.getLocalName() != null) {
            elementMap.put("_type", node.getLocalName());
        }

        // Set the attributes
        if (node.getAttributes() != null) {
            NamedNodeMap attributeMap = node.getAttributes();
            for (int i = 0; i < attributeMap.getLength(); i++) {
                Node attribute = attributeMap.item(i);
                elementMap.put(attribute.getNodeName(), attribute.getNodeValue());
            }
        }
    }

    /**
     * Handle TEXT nodes and CDATA nodes
     *
     * @param node
     * @param elementMap
     */
    private void handleTextNode(Node node, Map<String, Object> elementMap) {
        Object text = "";
        int nodeType = node.getNodeType();
        switch (nodeType) {
            case Node.TEXT_NODE:
                text = normalizeText(node.getNodeValue());
                break;
            case Node.CDATA_SECTION_NODE:
                text = normalizeText(((CharacterData) node).getData());
                break;
            default:
                break;
        }

        // If the text is valid ...
        if (!StringUtils.isEmpty(text.toString())) {
            // We check if we have already collected some text previously
            Object previousText = elementMap.get("_text");
            if (previousText != null) {
                // If we just have a "_text" key than we need to collect to a List
                text = Arrays.asList(previousText.toString(), text);
            }
            elementMap.put("_text", text);
        }
    }

    /**
     * Remove trailing whitespaces and new line characters
     *
     * @param text
     * @return
     */
    private String normalizeText(String text) {
        String[] tokens = StringUtils.split(text, "\n");
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].trim();
        }

        return StringUtils.join(tokens, " ").trim();
    }
}
