package view;

import db.DBConnector;
import table.tableupdate.TableRowUpd;

/**
 * Created by milya on 14.01.16.
 */
public abstract class ViewPart {

    protected static final DBConnector conn = new DBConnector();

    public abstract boolean processUpdate(TableRowUpd update, String viewName);

    public abstract String getId();
}
