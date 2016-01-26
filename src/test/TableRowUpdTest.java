import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.tableupdate.CellUpd;
import table.tableupdate.TableRowUpd;
import table.value.Value;
import view.Selection;
import view.ViewField;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by milya on 14.01.16.
 */
public class TableRowUpdTest extends TestCase {
    Logger LOG = LoggerFactory.getLogger(TableRowUpdTest.class);

    public void testGetUpdatedViewFields() {
        ViewField vf1 = new ViewField("test", "c1", "f1", Value.TYPE.INTEGER);
        ViewField vf2 = new ViewField("test", "c1", "f2", Value.TYPE.STRING);
        ViewField vf3 = new ViewField("test", "c1", "f3", Value.TYPE.BYTES);

        CellUpd cu1 = new CellUpd(false, vf1, null);
        CellUpd cu2 = new CellUpd(false, vf2, null);
        CellUpd cu3 = new CellUpd(false, vf3, null);
        CellUpd cu4 = new CellUpd(false, vf1, null);

        TableRowUpd tu = new TableRowUpd("test", "0", new ArrayList<>(Arrays.asList(cu1, cu2, cu3)));


        Selection s = new Selection(new ArrayList<>(Arrays.asList(vf2, vf1)));


        LOG.debug("Table Update Fields: " + tu.getUpdatedFields());
        LOG.debug("View Fields: " + s.getFields());

        ArrayList<ViewField> getUpdatedFields = new ArrayList<>(Arrays.asList(vf1, vf2));

        assertEquals(tu.getUpdatedViewFields(s.getFields()), getUpdatedFields);

//        assertEquals(tu.getUpdatedViewFields(s.getFields()), new ArrayList<ViewField>());
    }

    public void testGetUnUpdatedViewFields() {
        ViewField vf1 = new ViewField("test", "c1", "f1", Value.TYPE.INTEGER);
        ViewField vf2 = new ViewField("test", "c1", "f2", Value.TYPE.STRING);
        ViewField vf3 = new ViewField("test", "c1", "f3", Value.TYPE.BYTES);

        CellUpd cu1 = new CellUpd(false, vf1, null);
        CellUpd cu2 = new CellUpd(false, vf2, null);
        CellUpd cu3 = new CellUpd(false, vf3, null);
        CellUpd cu4 = new CellUpd(false, vf1, null);

        TableRowUpd tu = new TableRowUpd("test", "0", new ArrayList<>(Arrays.asList(cu1, cu2)));


        Selection s = new Selection(new ArrayList<>(Arrays.asList(vf1, vf2)));


        LOG.debug("Table Update Fields: " + tu.getUpdatedFields());
        LOG.debug("View Fields: " + s.getFields());

        ArrayList<ViewField> getUnUpdatedViewFields = new ArrayList<>();
//        ArrayList<ViewField> getUnUpdatedViewFields = new ArrayList<>(Arrays.asList(vf2));

        assertEquals(tu.getUnUpdatedViewFields(s.getFields()), getUnUpdatedViewFields);

    }

    public void testAreViewFieldsUpdated() {
        ViewField vf1 = new ViewField("test", "c1", "f1", Value.TYPE.INTEGER);
        ViewField vf2 = new ViewField("test", "c1", "f2", Value.TYPE.STRING);
        ViewField vf3 = new ViewField("test", "c1", "f3", Value.TYPE.BYTES);

        CellUpd cu1 = new CellUpd(false, vf1, null);
        CellUpd cu2 = new CellUpd(false, vf2, null);
        CellUpd cu3 = new CellUpd(false, vf3, null);
        CellUpd cu4 = new CellUpd(false, vf1, null);

        TableRowUpd tu = new TableRowUpd("test", "0", new ArrayList<>(Arrays.asList(cu1, cu2)));


        Selection s = new Selection(new ArrayList<>(Arrays.asList(vf3)));


        LOG.debug("Table Update Fields: " + tu.getUpdatedFields());
        LOG.debug("View Fields: " + s.getFields());


        assertFalse(tu.areViewFieldsUpdated(s.getFields()));

    }

    public void testGetUpdatedViewCells() {
        ViewField vf1 = new ViewField("test", "c1", "f1", Value.TYPE.INTEGER);
        ViewField vf2 = new ViewField("test", "c1", "f2", Value.TYPE.STRING);
        ViewField vf3 = new ViewField("test", "c1", "f3", Value.TYPE.BYTES);

        CellUpd cu1 = new CellUpd(false, vf1, null);
        CellUpd cu2 = new CellUpd(false, vf2, null);
        CellUpd cu3 = new CellUpd(false, vf3, null);
        CellUpd cu4 = new CellUpd(false, vf1, null);

        TableRowUpd tu = new TableRowUpd("test", "0", new ArrayList<>(Arrays.asList(cu1, cu2)));
        Selection s = new Selection(new ArrayList<>(Arrays.asList(vf1, vf3)));

        ArrayList<CellUpd> getUpdatedViewCells = new ArrayList<>(Arrays.asList(cu1));

        assertEquals(tu.getUpdatedViewCells(s.getFields()), getUpdatedViewCells);

    }

}
