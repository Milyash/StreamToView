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
import view.grouped.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
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
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
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

    private static Selection parseSelection(Element selectElement, String tName) {
        if (selectElement == null) return null;
        Selection selection = new Selection();

        NodeList selects = selectElement.getElementsByTagName("field");
        for (int i = 0; i < selects.getLength(); i++) {
            Element selectFieldElement = (Element) selects.item(i);
            ViewField field = getViewField(selectFieldElement, tName);
            if (field == null) return null;
            selection.addField(field);
        }
        return (selection.getFields().size() > 0) ? selection : null;
    }

    private static ArrayList<ViewField> parseGroupsBy(Element selectElement, String tName) {
        if (selectElement == null) return null;
        ArrayList<ViewField> maxs = new ArrayList<ViewField>();

        NodeList groupByNodes = ((Element) selectElement.getParentNode()).getElementsByTagName("groupby");
        for (int i = 0; i < groupByNodes.getLength(); i++) {
            Element groupByElement = (Element) groupByNodes.item(i);
            String groupByTable = groupByElement.getAttribute("table");
            if (groupByTable != null && !groupByTable.equals(tName)) continue;

            ViewField field = getViewField(groupByElement, tName);
            if (field == null) return null;

            LOG.error(" *********************** TableXMLViewFactory group by parsed: " + field.toString());
            maxs.add(field);
        }
        return maxs;
    }


    private static <T> ArrayList<T> parseAggregate(Element selectElement, String tName, String aggregateName) {
        if (selectElement == null) return null;
        ArrayList<T> aggregates = new ArrayList<T>();

        NodeList aggregatesNodes = selectElement.getElementsByTagName(aggregateName);
        for (int i = 0; i < aggregatesNodes.getLength(); i++) {
            Element aggregateElement = (Element) aggregatesNodes.item(i);
            ViewField field = getViewField(aggregateElement, tName);
            if (field == null) return null;

            ArrayList<ViewField> groupByFields = parseGroupsBy(selectElement, tName);

            ViewField groupByField = (groupByFields.size() > 0) ? groupByFields.get(0) : null;

            LOG.error(" *********************** TableXMLViewFactory " + aggregateName + " parsed: " + field.toString());
            aggregates.add((T) Aggregate.createAggregate(field, groupByField, aggregateName));
        }
        return (aggregates.size() > 0) ? aggregates : null;
    }

    private static Having parseHaving(Element havingElement, String tName) {
        if (havingElement == null) return null;

        Aggregate aggregate = null;
        NodeList aggregatesNodes = havingElement.getElementsByTagName("max");
        if (aggregatesNodes.getLength() == 0)
            aggregatesNodes = havingElement.getElementsByTagName("min");
        if (aggregatesNodes.getLength() == 0)
            aggregatesNodes = havingElement.getElementsByTagName("sum");
        if (aggregatesNodes.getLength() == 0)
            aggregatesNodes = havingElement.getElementsByTagName("count");

        if (aggregatesNodes.getLength() == 0) return null;
        Element aggregateElement = (Element) aggregatesNodes.item(0);
        String aggregateType = aggregateElement.getTagName();
        ViewField aggregateField = getViewField(aggregateElement, tName);
        switch (aggregateType) {
            case "max":
                aggregate = new Max(aggregateField);
                break;
            case "min":
                aggregate = new Min(aggregateField);
                break;
            case "sum":
                aggregate = new Sum(aggregateField);
                break;
            case "count":
                aggregate = new Count(aggregateField);
                break;
        }

        Element conditionElement = (Element) havingElement.getElementsByTagName("condition").item(0);

        if (conditionElement.hasAttribute("type") && conditionElement.hasAttribute("value")) {
            String operand = conditionElement.getAttribute("type");
            Integer argument = Integer.parseInt(conditionElement.getAttribute("value"));

            Condition condition = new Condition(tName, aggregate.FIELD_TO_SELECTION, argument);

            return new Having(ConditionFactory.getCondition(condition, operand));
        }

        return null;
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
        List<View> views = new ArrayList<>();
        try {
            HashSet<String> viewsParsed = new HashSet<>();
            XPathExpression expr =
                    xpath.compile("/views/view/select/*[@table='" + tableName + "']");
            NodeList viewsList = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

            for (int i = 0; i < viewsList.getLength(); i++) { // views
                Element viewElement = (Element) viewsList.item(i).getParentNode().getParentNode();

                String viewId = viewElement.getAttribute("name");
                if (viewId != null && viewsParsed.contains(viewId))
                    continue;
                else
                    viewsParsed.add(viewId);

                View view = new View();

                Element selectNode = (Element) viewElement.getElementsByTagName("select").item(0);
                Selection selection = parseSelection(selectNode, tableName);

                ArrayList<Max> maxes = parseAggregate(selectNode, tableName, "max");
                ArrayList<Min> mins = parseAggregate(selectNode, tableName, "min");
                ArrayList<Sum> sums = parseAggregate(selectNode, tableName, "sum");
                ArrayList<Count> counts = parseAggregate(selectNode, tableName, "count");

                Element whereElement = (Element) viewElement.getElementsByTagName("where").item(0);
                Where where = parseWhere(whereElement, tableName);

                Element havingElement = (Element) viewElement.getElementsByTagName("having").item(0);
                LOG.error(getInnerString(viewElement.getElementsByTagName("having").item(0)));
                Having having = parseHaving(havingElement, tableName);

                if (selection == null && maxes == null && mins == null && sums == null && counts == null)
                    continue;

                view.setSelection(selection);
                if (maxes != null) view.setMaxes(maxes);
                if (mins != null) view.setMins(mins);
                if (sums != null) view.setSums(sums);
                if (counts != null) view.setCounts(counts);


                if (where != null && !where.isEmpty())
                    view.setWhere(where);

                if (having != null)
                    view.setHaving(having);

                view.updateActualName();

                LOG.error(" *********************** TableXMLViewFactory view parsed: " + view);

                views.add(view);
            }
        } catch (XPathExpressionException e) {
            LOG.error("Table " + tableName + " is not found!");
        }

        return views;
    }

    private static String getInnerString(Node node) {
        try {
            TransformerFactory transFactory = TransformerFactory.newInstance();
            Transformer transformer = transFactory.newTransformer();
            StringWriter buffer = new StringWriter();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.transform(new DOMSource(node),
                    new StreamResult(buffer));
            return buffer.toString();
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        }
        return null;
    }

}
