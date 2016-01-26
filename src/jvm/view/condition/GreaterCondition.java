package view.condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import view.ViewField;

/**
 * Created by milya on 19.12.15.
 */
public class GreaterCondition extends Condition {
    private static Logger LOG = LoggerFactory.getLogger(GreaterCondition.class);

    public GreaterCondition() {
        type = "_greater_";
    }

    public GreaterCondition(ViewField field, boolean isValueArgument, ViewField fieldArgument, Object valueArgument) {
        super(field, isValueArgument, fieldArgument, valueArgument);
        type = "_greater_";
    }

    public GreaterCondition(ViewField field, ViewField fieldArgument) {
        super(field, fieldArgument);
    }

    public GreaterCondition(String tableName, ViewField field, Object valueArgument) {
        super(tableName, field, valueArgument);
    }

    public GreaterCondition(Condition condition) {
        super(condition);
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public boolean evaluate(Object valueToEvaluate) {
        if (valueToEvaluate == null) return false;

        Object argument = getParameterValue();
        if (valueToEvaluate instanceof Integer)
            return (Integer) valueToEvaluate > (Integer) argument;

        else if (valueToEvaluate instanceof String)
            return ((String) valueToEvaluate).compareTo((String) argument) > 0;

        return false;
    }

    @Override
    public String toString() {
        return "GreaterCondition{" + super.toString() + "}";
    }
}
