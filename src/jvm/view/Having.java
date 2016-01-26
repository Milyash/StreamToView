package view;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.TableRowUpd;
import view.condition.Condition;

import java.io.IOException;

/**
 * Created by milya on 19.12.15.
 */
public class Having extends ViewPart {
    private static Logger LOG = LoggerFactory.getLogger(Having.class);
    private static final String VIEW_TYPE = "having";
    protected Condition condition;

    public Having() {
    }

    public Having(Condition condition) {
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
        String id = VIEW_TYPE + "-" + condition.getId();
        return id;
    }

    @Override
    public String toString() {
        return "Having{" +
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
                LOG.error("================== HAVING processUpdate : " + !update.getTableName().equals(getField().getTableName())
                        + " \n doesn't update having field " + !update.areViewFieldsUpdated(getField()));
                return isFieldInTableMeetsCondition(update, viewName);
            }

            return checkCondition(update);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean checkCondition(TableRowUpd update) {
//        try {
        Object valueToEvaluate = update.getUpdatedValueByField(condition.getField());
        LOG.error("================== HAVING checkCondition: update " + update + " value reived: " + valueToEvaluate + " ? " + condition.getValueArgument());

        boolean isConditionMet = condition.evaluate(valueToEvaluate);

        LOG.error("================== HAVING checkCondition isConditionMet: " + isConditionMet + " ");

        return isConditionMet;

    }

    private boolean isFieldInTableMeetsCondition(TableRowUpd update, String viewName) throws IOException {
        String tableName = viewName;
        String pk = update.getPk();
        Object entryFieldValue = conn.getFieldByPk(tableName, pk, condition.getField());
        if (entryFieldValue == null) return false;
        return condition.evaluate(entryFieldValue);
    }
}
