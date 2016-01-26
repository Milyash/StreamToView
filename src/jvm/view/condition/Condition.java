package view.condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import view.ViewField;

/**
 * Created by milya on 19.12.15.
 */
public class Condition {
    private static Logger LOG = LoggerFactory.getLogger(Condition.class);
    private ViewField field;
    protected String type;
    private boolean isValueArgument;
    private ViewField fieldArgument;
    private Object valueArgument;

    public Condition() {
    }

    public Condition(ViewField field, boolean isValueArgument, ViewField fieldArgument, Object valueArgument) {
        this.field = field;
        this.isValueArgument = isValueArgument;
        this.fieldArgument = fieldArgument;
        this.valueArgument = valueArgument;
    }

    public Condition(ViewField field, ViewField fieldArgument) {
        this.field = field;
        this.isValueArgument = false;
        this.fieldArgument = fieldArgument;
    }

    public Condition(String tableName, ViewField field, Object valueArgument) {
        this.field = field;
        this.isValueArgument = true;
        this.valueArgument = valueArgument;
    }

    public Condition(Condition condition) {
        this.field = condition.getField();
        this.isValueArgument = condition.isValueArgument();
        this.fieldArgument = condition.getFieldArgument();
        this.valueArgument = condition.getValueArgument();
    }

    public ViewField getField() {
        return field;
    }

    public void setField(ViewField field) {
        this.field = field;
    }

    public boolean isValueArgument() {
        return isValueArgument;
    }

    public void setIsValueParameter(boolean isValueParameter) {
        this.isValueArgument = isValueParameter;
    }

    public ViewField getFieldArgument() {
        return fieldArgument;
    }

    public void setFieldArgument(ViewField fieldArgument) {
        this.fieldArgument = fieldArgument;
    }

    public Object getValueArgument() {
        return valueArgument;
    }

    public void setValueArgument(Object valueArgument) {
        this.valueArgument = valueArgument;
    }

    public String getType() {
        return "General condition";
    }

    public String getId() {
        StringBuilder sb = new StringBuilder(field.getTableName() + "-"
                + field.getFamilyName() + "-"
                + field.getColumnName() + "-"
                + type + "-"
                + valueArgument.toString());
        return sb.toString();
    }

    public Object getParameterValue() {
        if (isValueArgument) return valueArgument;
        return fieldArgument;
    }

    public boolean evaluate(Object valueToEvaluate) {
        LOG.error("------------------- CONDITION: false evaluator");
        return false;
    }

    public boolean isEmpty() {
        return field == null;
    }

    @Override
    public String toString() {
        return "Condition{" +
                "field=" + field +
                ", isValueArgument=" + isValueArgument +
                ", fieldArgument=" + fieldArgument +
                ", valueArgument=" + valueArgument +
                '}';
    }
}
