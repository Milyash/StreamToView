package view.condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import view.ViewField;

/**
 * Created by milya on 19.12.15.
 */
public class LessOrEqualsCondition extends Condition {
    private static Logger LOG = LoggerFactory.getLogger(LessOrEqualsCondition.class);
    private static final String type = "_lessOrEquals_";

    public LessOrEqualsCondition() {
    }

    public LessOrEqualsCondition(ViewField field, boolean isValueArgument, ViewField fieldArgument, Object valueArgument) {
        super(field, isValueArgument, fieldArgument, valueArgument);
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

        String argument = (String) getParameterValue();
        if (valueToEvaluate instanceof Integer)
            return (Integer) valueToEvaluate <= Integer.parseInt(argument);

        else if (valueToEvaluate instanceof String)
            return ((String) valueToEvaluate).compareTo(argument) <= 0;

        return false;
    }

    @Override
    public String toString() {
        return "LessOrEqualsCondition{" + super.toString() + "}";
    }
}
