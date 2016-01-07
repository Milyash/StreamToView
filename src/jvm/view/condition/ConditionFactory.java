package view.condition;

import view.condition.*;

/**
 * Created by milya on 19.12.15.
 */
public class ConditionFactory {
    public static Condition getCondition(Condition condition, String conditionType) {
        switch (conditionType) {
            case "greater":
                return new GreaterCondition(condition);
            case "less":
                return new LessCondition(condition);
            case "equals":
                return new EqualsCondition(condition);
            case "greaterEquals":
                return new GreaterOrEqualsCondition(condition);
            case "lessEquals":
                return new LessOrEqualsCondition(condition);
        }
        return null;
    }


}
