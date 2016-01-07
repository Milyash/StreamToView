package utils;

import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import table.Field;
import table.Table;
import table.value.Value;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.IOException;
import java.util.*;

/**
 * Created by milya on 22.11.15.
 */
public class TableXMLDefinitionFacotry {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TableXMLDefinitionFacotry.class);
    private static final String LOG_STRING = "------ TableXMLDefinitionFacotry: ";
    private static Document doc;
    private static XPath xpath = XPathFactory.newInstance().newXPath();

    private static void init() {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();
            doc = builder.parse("src/main/resources/db-schema.xml");
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<Field> getFieldsDefinitionByTableName(String tableName) {
        init();
        List<Field> fields = new ArrayList<>();
        try {
            //create XPathExpression object
            XPathExpression expr =
                    xpath.compile("/tables/table[@name='" + tableName + "']/columnFamily");
            //evaluate expression result on XML document
            NodeList families = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

            for (int i = 0; i < families.getLength(); i++) { // column families
                Node familyNode = families.item(i);
                if (familyNode.getNodeType() != Node.ELEMENT_NODE) continue;
                Element family = (Element) familyNode;

                String familyName = family.getAttribute("name");

                NodeList columns = family.getElementsByTagName("column");

                for (int j = 0; j < columns.getLength(); j++) { // columns
                    Node columnNode = columns.item(j);
                    if (columnNode.getNodeType() != Node.ELEMENT_NODE) continue;
                    Element column = (Element) columnNode;

                    String columnName = column.getAttribute("name");
                    Value.TYPE dataType = Value.getTypeByKey(column.getAttribute("dataType"));

                    Field field = new Field(columnName, familyName, dataType);
                    LOG.error(LOG_STRING + " getFieldsDefinitionByTableName field: " + familyName + ":" + columnName + " of type " + dataType);
                    fields.add(field);

                }
            }
        } catch (XPathExpressionException e) {
            LOG.error("Table " + tableName + " is not found!");
        }
        return fields;
    }

    public static Table getTableDefinitionByTableName(String tableName) {
        List<Field> fields = getFieldsDefinitionByTableName(tableName);
        if (fields.isEmpty()) return null;
        Table table = new Table(tableName, fields);
        return table;
    }


    public static Value.TYPE getFieldDataType(String tableName, String columnFamily, String columnName) {
        init();
        try {
            XPathExpression expr =
                    xpath.compile("/tables/table[@name='" + tableName + "']/columnFamily[@name='" + columnFamily + "']/column[@name='" + columnName + "']");

            Element columnDefinition = (Element) ((NodeList) expr.evaluate(doc, XPathConstants.NODESET)).item(0);
            if (columnDefinition == null) return null; // todo raise exception

            LOG.error(LOG_STRING + " getFieldDataType: " + tableName + "::" + columnFamily + ":" + columnName + "->" + columnDefinition.getAttribute("dataType"));
            return Value.getTypeByKey(columnDefinition.getAttribute("dataType"));

        } catch (XPathExpressionException e) {
            LOG.error("Table " + tableName + " is not found!");
        }
        return Value.TYPE.BYTES;
    }

}
