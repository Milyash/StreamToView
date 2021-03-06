package view;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.TableRowUpd;
import view.condition.Condition;

import java.io.IOException;

/**
 * Created by milya on 19.12.15.
 */
public class Where extends ViewPart {
    private static Logger LOG = LoggerFactory.getLogger(Where.class);
    private static final String VIEW_TYPE = "condition";
    protected Condition condition;

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

    @Override
    public String getId() {
        StringBuilder sb = new StringBuilder(VIEW_TYPE + "-" + condition.getId());
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

    @Override
    public boolean processUpdate(TableRowUpd update, String viewName) {

        try {
            if (!update.areViewFieldsUpdated(getField())) {
                LOG.error("================== WHERE processUpdate : " + !update.getTableName().equals(getField().getTableName()) + " doesn't update where field " + !update.areViewFieldsUpdated(getField()));
//                return conn.isPkInTable(viewName, update.getPk());
                return isFieldInTableMeetsCondition(update);
            }

//            if (update instanceof TableRowPutUpd) {
            return checkCondition(update, viewName);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean checkCondition(TableRowUpd update, String viewName) {
//        try {
        Object valueToEvaluate = update.getUpdatedValueByField(condition.getField());
        LOG.error("================== WHERE checkCondition: update " + update + " value reived: " + valueToEvaluate + " ? " + condition.getValueArgument());

        boolean isConditionMet = condition.evaluate(valueToEvaluate);


        LOG.error("================== WHERE checkCondition isConditionMet: " + isConditionMet + " ");

        return isConditionMet;
//            if (isConditionMet)
//                return true;
//            else {
//                LOG.error("================== WHERE checkCondition delete pk: " + update.getPk() + " view: " + viewName);
//                conn.deleteRowByPk(viewName, update.getPk());
//                return false;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return false;
    }

    private boolean isFieldInTableMeetsCondition(TableRowUpd update) throws IOException {
        String tableName = update.getTableName();
        String pk = update.getPk();
        Object entryFieldValue = conn.getFieldByPk(tableName, pk, condition.getField());
        if (entryFieldValue == null) return false;
        return condition.evaluate(entryFieldValue);
    }
}
