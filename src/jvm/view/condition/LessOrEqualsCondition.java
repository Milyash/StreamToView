package view.condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import view.ViewField;

/**
 * Created by milya on 19.12.15.
 */
public class LessOrEqualsCondition extends Condition {
    private static Logger LOG = LoggerFactory.getLogger(LessOrEqualsCondition.class);

    public LessOrEqualsCondition() {
        type = "_lessOrEquals_";
    }

    public LessOrEqualsCondition(ViewField field, boolean isValueArgument, ViewField fieldArgument, Object valueArgument) {
        super(field, isValueArgument, fieldArgument, valueArgument);
        type = "_lessOrEquals_";
    }

    public LessOrEqualsCondition(ViewField field, ViewField fieldArgument) {
        super(field, fieldArgument);
    }

    public LessOrEqualsCondition(String tableName, ViewField field, Object valueArgument) {
        super(tableName, field, valueArgument);
    }

    public LessOrEqualsCondition(Condition condition) {
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
            return (Integer) valueToEvaluate <= (Integer) argument;

        else if (valueToEvaluate instanceof String)
            return ((String) valueToEvaluate).compareTo((String) argument) <= 0;

        return false;
    }

    @Override
    public String toString() {
        return "LessOrEqualsCondition{" + super.toString() + "}";
    }
}