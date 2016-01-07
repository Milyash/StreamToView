package view;

import db.DBConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.*;
import view.condition.Condition;

import java.io.IOException;

/**
 * Created by milya on 19.12.15.
 */
public class Where {
    private static Logger LOG = LoggerFactory.getLogger(Where.class);
    private static final String VIEW_TYPE = "condition";
    protected Condition condition;
    protected static DBConnector conn = new DBConnector();

    public Where() {
    }

    public Where(Condition condition) {
        this.condition = condition;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public boolean isEmpty() {
        return condition == null || condition.isEmpty();
    }

    public String createTableName() {
        StringBuilder sb = new StringBuilder(VIEW_TYPE + "_" + condition.getField().getTableName() + "_" + condition.getField().getFamilyName() + "-" + condition.getField().getColumnName() + condition.getType() + condition.getValueArgument().toString());

        return sb.toString();
    }


    @Override
    public String toString() {
        return "Where{" +
                "condition=" + condition +
                '}';
    }

    public ViewField getField() {
        return condition.getField();
    }

    public void setField(ViewField field) {
        condition.setField(field);
    }

    public boolean isValueArgument() {
        return condition.isValueArgument();
    }

    public void setIsValueParameter(boolean isValueParameter) {
        condition.setIsValueParameter(isValueParameter);
    }

    public ViewField getFieldArgument() {
        return condition.getFieldArgument();
    }

    public void setFieldArgument(ViewField fieldArgument) {
        condition.setFieldArgument(fieldArgument);
    }

    public Object getValueArgument() {
        return condition.getValueArgument();
    }

    public void setValueArgument(Object valueArgument) {
        condition.setValueArgument(valueArgument);
    }

    public boolean processUpdate(TableRowUpd update, String viewName) {

        if (!update.getTableName().equals(getField().getTableName())
                || !update.areFieldsFromListUpdated(getField())) {
            LOG.error("================== WHERE processUpdate: " + !update.getTableName().equals(getField().getTableName()) + " || " + !update.areFieldsFromListUpdated(getField()));
            return false;
        }
        try {

            DBConnector conn = new DBConnector();
            if (update instanceof TableRowPutUpd) {
                return checkCondition(update, viewName);

            } else if (update instanceof TableRowDeleteUpd) {

                if (conn.isPkInTable(viewName, update.getPk())) {
                    conn.deleteRowByPk(viewName, update.getPk());
                    return false;
                } else {
                    boolean r = checkCondition(update, viewName);
                    LOG.error("================== WHERE processUpdate: " + update + " PASS: " + r);
                    return r;
                }
            }
            return false;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean checkCondition(TableRowUpd update, String viewName) {
        try {
            Object valueToEvaluate = update.getUpdatedValueByField(condition.getField());
            LOG.error("================== WHERE checkCondition: update " + update + " value reived: " + valueToEvaluate + " ? " + condition.getValueArgument());

            boolean isConditionMet = condition.evaluate(valueToEvaluate);


            LOG.error("================== WHERE checkCondition isConditionMet: " + isConditionMet + " ");

            if (isConditionMet)
                return true;
            else {
                LOG.error("================== WHERE checkCondition delete pk: " + update.getPk() + " view: " + viewName);
                conn.deleteRowByPk(viewName, update.getPk());
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
