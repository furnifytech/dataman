package com.furnify.dataman.model;

import com.furnify.dataman.constants.Constants;
import lombok.Getter;
import lombok.ToString;
import org.w3c.dom.CharacterData;
import org.w3c.dom.*;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ToString
@XmlAccessorType(XmlAccessType.FIELD)
public class Table {
    @Getter
    @XmlAttribute(name = "name")
    private String name;

    @Getter
    @XmlAttribute(name = "exception_list")
    private String exceptionList;

    @Getter
    @XmlAttribute(name = "updatable")
    private boolean updatable = true;

    @Getter
    @XmlElement(name = "meta")
    private Meta meta;

    @Getter
    @XmlElement(name = "meta-type")
    private Meta metaType;

    @Getter
    @XmlElement(name = "default-value")
    private DefaultValue defaultValue;

    @XmlAnyElement(lax = true)
    private List<Element> row;

    public List<Map<String, String>> getRows(String profile) {
        List<Map<String, String>> rows = new ArrayList<>();
        for (Element element : row) {
            Map<String, String> mapData = new HashMap<>();

            // Read all the attributes
            extractAttributes(element, mapData, profile);

            // Read all the Child Nodes
            extractChildNodes(element, mapData, profile);

            rows.add(mapData);
        }
        return rows;
    }

    private void extractChildNodes(Element element, Map<String, String> mapData, String profile) {
        NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (nodeList.item(i).getNodeType() == Node.ELEMENT_NODE) {
            	if (!checkProfile(nodeList.item(i).getAttributes(), profile)) {
            		continue;
				}

                if (nodeList.item(i) instanceof CharacterData) {
                    CharacterData data = (CharacterData) nodeList.item(i);
                    mapData.put(data.getNodeName().trim(), data.getData().trim());
                } else {
                    mapData.put(nodeList.item(i).getNodeName().trim(), nodeList.item(i).getTextContent().trim());
                }
            }
        }
    }

    private void extractAttributes(Element element, Map<String, String> mapData, String profile) {
        NamedNodeMap attrMap = element.getAttributes();

        if (!checkProfile(attrMap, profile)) {
        	return;
		}

		for (int i = 0; i < attrMap.getLength(); i++) {
			Node attr = attrMap.item(i);
			mapData.put(attr.getNodeName().trim(), attr.getNodeValue().trim());
		}
    }

    private boolean checkProfile(NamedNodeMap namedNodeMap, String profile) {
        if (profile != null) {
			for (int i = 0; i < namedNodeMap.getLength(); i++) {
				Node attr = namedNodeMap.item(i);

				if (Constants.PROFILE.equalsIgnoreCase(attr.getNodeName().trim())) {
					return profile.equalsIgnoreCase(attr.getNodeValue().trim());
				}
			}
		}

        return true;
    }
}
