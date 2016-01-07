package view;

/**
 * Created by milya on 19.12.15.
 */
public class View {
    protected String name;
    private Selection selection;
    private Where where;

    public View() {
    }

    public View(String name) {
        this.name = name;
    }

    public View(String name, Selection selection, Where where) {
        this.name = name;
        this.selection = selection;
        this.where = where;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Selection getSelection() {
        return selection;
    }

    public void setSelection(Selection selection) {
        this.selection = selection;
    }

    public void addSelectionField(ViewField field) {
        this.selection.getFields().add(field);
    }

    public Where getWhere() {
        return where;
    }

    public void setWhere(Where where) {
        this.where = where;
    }

    public void updateActualName() {
        name = selection.createTableName();
        if (where != null && !where.isEmpty())
            name += where.createTableName();
    }

    @Override
    public String toString() {
        return "View{" +
                "name='" + name + '\'' +
                ", selection=" + selection +
                ", where=" + where +
                '}';
    }
}
