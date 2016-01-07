package table.value;

import org.apache.hadoop.hbase.util.Bytes;
import table.exceptions.TableValueUnknownTypeException;

import java.io.Serializable;

/**
 * Created by milya on 16.11.15.
 */
public abstract class Value implements Serializable {

    protected byte[] byteValue;

    public enum TYPE {
        STRING, INTEGER, BYTES;
    }

    public static TYPE getTypeByKey(String key) {
        switch (key) {
            case "int":
                return TYPE.INTEGER;
            case "string":
                return TYPE.STRING;
        }
        return TYPE.BYTES;
    }

    public abstract Object getValue();

    public byte[] getByteValue(){
        return byteValue;
    }

    public abstract int compareTo(Value value);

}
