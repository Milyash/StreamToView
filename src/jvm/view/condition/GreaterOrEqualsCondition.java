package view.condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import view.ViewField;

/**
 * Created by milya on 19.12.15.
 */
public class GreaterOrEqualsCondition extends Condition {
    private static Logger LOG = LoggerFactory.getLogger(GreaterOrEqualsCondition.class);

    public GreaterOrEqualsCondition() {
        type = "_greaterOrEquals_";
    }

    public GreaterOrEqualsCondition(ViewField field, boolean isValueArgument, ViewField fieldArgument, Object valueArgument) {
        super(field, isValueArgument, fieldArgument, valueArgument);
        type = "_greaterOrEquals_";
    }

    public GreaterOrEqualsCondition(ViewField field, ViewField fieldArgument) {
        super(field, fieldArgument);
    }

    public GreaterOrEqualsCondition(String tableName, ViewField field, Object valueArgument) {
        super(tableName, field, valueArgument);
    }

    public GreaterOrEqualsCondition(Condition condition) {
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
            return (Integer) valueToEvaluate >= (Integer) argument;

        else if (valueToEvaluate instanceof String)
            return ((String) valueToEvaluate).compareTo((String) argument) >= 0;

        return false;
    }

    @Override
    public String toString() {
        return "GreaterOrEqualsCondition{" + super.toString() + "}";
    }
}
