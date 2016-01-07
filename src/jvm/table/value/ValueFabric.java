package table.value;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.exceptions.TableValueUnknownTypeException;

/**
 * Created by milya on 21.11.15.
 */
public class ValueFabric {
    private static Logger LOG = LoggerFactory.getLogger(ValueFabric.class);

    public static Object getValue(byte[] bytes, Value.TYPE dataType) {
        if (bytes == null) return null;
        String valueString = Bytes.toString(bytes);
        try {
            switch (dataType) {
                case STRING:
                    return valueString;
                case INTEGER:
                    return Integer.parseInt(valueString);
            }
            return bytes;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
