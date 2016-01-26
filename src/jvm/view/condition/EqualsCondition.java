package view.condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import view.ViewField;

/**
 * Created by milya on 19.12.15.
 */
public class EqualsCondition extends Condition {
    private static Logger LOG = LoggerFactory.getLogger(EqualsCondition.class);

    public EqualsCondition() {
        type = "_equals_";
    }

    public EqualsCondition(ViewField field, boolean isValueArgument, ViewField fieldArgument, Object valueArgument) {
        super(field, isValueArgument, fieldArgument, valueArgument);
        type = "_equals_";
    }

    public EqualsCondition(ViewField field, ViewField fieldArgument) {
        super(field, fieldArgument);
    }

    public EqualsCondition(String tableName, ViewField field, Object valueArgument) {
        super(tableName, field, valueArgument);
    }

    public EqualsCondition(Condition condition) {
        super(condition);
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public boolean evaluate(Object valueToEvaluate) {

        Object argument = getParameterValue();
        if (valueToEvaluate == null) return false;

        if (valueToEvaluate instanceof Integer)
            return valueToEvaluate.equals(argument);

        else if (valueToEvaluate instanceof String)
            return ((String) valueToEvaluate).equals(argument);

        return false;
    }

    @Override
    public String toString() {
        return "EqualsCondition{ " + super.toString() + "}";
    }
}
