package utils;

import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import table.value.Value;
import view.*;
import view.condition.Condition;
import view.condition.ConditionFactory;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by milya on 27.11.15.
 */
public class TableXMLViewFactory {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TableXMLViewFactory.class);
    private static Document doc;
    private static XPath xpath = XPathFactory.newInstance().newXPath();


    private static void init() {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();
//            doc = builder.parse("src/main/resources/db-schema.xml");
            doc = builder.parse("src/main/resources/views.xml");
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static ViewField getViewField(Element element, String tName) {
        String columnFamily = element.getAttribute("columnFamily");
        String columnName = element.getAttribute("name");
        String tableName = element.getAttribute("table");
        if (tableName.equals("")) tableName = tName;
        Value.TYPE dataType = TableXMLDefinitionFacotry.getFieldDataType(tableName, columnFamily, columnName);
        if (dataType != null)
            return new ViewField(tableName, columnName, columnFamily, dataType);
        return null;
    }

    private static Selection parseSelection(Node selectNode, String tName) {
        if (selectNode == null) return null;
        Selection selection = new Selection(tName);

        NodeList selectFieldNodes = ((Element) selectNode).getElementsByTagName("field");
        for (int i = 0; i < selectFieldNodes.getLength(); i++) {
            Element selectFieldElement = (Element) selectFieldNodes.item(i);
            ViewField field = getViewField(selectFieldElement, tName);
            if (field == null) return null;
            selection.addField(field);
        }
        return selection;
    }

    private static Where parseWhere(Element whereElement, String tableName) {

        if (whereElement == null) return null;

        Where where = new Where();

        NodeList conditions = whereElement.getElementsByTagName("condition");

        for (int i = 0; i < conditions.getLength(); i++) {
            if (conditions.item(i).getNodeType() != Node.ELEMENT_NODE) continue;

            Condition condition = new Condition();
            Element conditionElement = (Element) conditions.item(i);

            //field
            Element fieldElement = (Element) conditionElement.getElementsByTagName("field").item(0);
            condition.setField(getViewField(fieldElement, tableName));

            Element argumentElement = (Element) conditionElement.getElementsByTagName("argument").item(0);

            //argument type
            boolean isValueArgument = argumentElement.getAttribute("type").equals("value");
            condition.setIsValueParameter(isValueArgument);

            //argument value
            if (isValueArgument) {
                Element valueArgumentElement = (Element) argumentElement.getElementsByTagName("value").item(0);
                condition.setValueArgument(valueArgumentElement.getTextContent());
            } else {
                Element fieldArgumentElement = (Element) argumentElement.getElementsByTagName("field").item(0);
                condition.setValueArgument(getViewField(fieldArgumentElement, tableName));
            }

            //operand
            Element operandElement = (Element) conditionElement.getElementsByTagName("operand").item(0);
            String operand = operandElement.getTextContent();
            Condition typedCondition = ConditionFactory.getCondition(condition, operand);

            where.setCondition(typedCondition); // todo add conditions

        }
        return where;
    }


    public static List<View> getViewsByTableName(String tableName) {
        init();
        List<View> views = new ArrayList<View>();
        try {
            XPathExpression expr =
                    xpath.compile("/views/view/select/field[@table='" + tableName + "']");
            NodeList viewsList = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

            for (int i = 0; i < viewsList.getLength(); i++) { // views
                Element viewElement = (Element) viewsList.item(i).getParentNode().getParentNode();

                View view = new View();

                Node selectNode = viewElement.getElementsByTagName("select").item(0);
                Selection selection = parseSelection(selectNode, tableName);

                Element whereElement = (Element) viewElement.getElementsByTagName("where").item(0);
                Where where = parseWhere(whereElement, tableName);

                if (selection == null || selection.isEmpty())
                    continue;
                view.setSelection(selection);

                if (where == null || !where.isEmpty()) {
                    view.setWhere(where);

                    LOG.error(" *********************** TableXMLViewFactory where: " + view);
                }

                LOG.error(" *********************** TableXMLViewFactory where: " + view);
                view.updateActualName();
                views.add(view);
            }
        } catch (XPathExpressionException e) {
            LOG.error("Table " + tableName + " is not found!");
        }

        LOG.error(" *********************** TableXMLViewFactory views: " + views.toString());
        return views;
    }

}
