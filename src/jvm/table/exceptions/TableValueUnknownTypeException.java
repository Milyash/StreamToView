package table.exceptions;

/**
 * Created by milya on 16.11.15.
 */
public class TableValueUnknownTypeException extends Exception {
    public TableValueUnknownTypeException() {}

    public TableValueUnknownTypeException(String message) {
        super(message);
    }
}
