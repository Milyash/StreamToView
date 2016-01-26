package view;

import db.DBConnector;
import table.value.Value;
import view.grouped.*;

import java.util.ArrayList;

/**
 * Created by milya on 19.12.15.
 */
public class View {
    protected String name;
    private Selection selection;
    private ArrayList<Min> mins;
    private ArrayList<Max> maxes;
    private ArrayList<Sum> sums;
    private ArrayList<Count> counts;
    private Where where;
    private Having having;

    protected static final DBConnector conn = new DBConnector();

    public View() {

    }

    public View(String name) {
        this.name = name;
    }

    public View(String name, Selection selection,
                ArrayList<Min> mins,
                ArrayList<Max> maxes,
                ArrayList<Sum> sums,
                ArrayList<Count> counts,
                Where where,
                Having having) {
        this.name = name;
        this.selection = selection;
        this.mins = mins;
        this.maxes = maxes;
        this.sums = sums;
        this.counts = counts;
        this.where = where;
        this.having = having;
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

    public ArrayList<Max> getMaxes() {
        return maxes;
    }

    public void setMaxes(ArrayList<Max> maxes) {
        this.maxes = maxes;
        addAggregate(maxes);
    }

    public ArrayList<Min> getMins() {
        return mins;
    }

    public void setMins(ArrayList<Min> mins) {
        this.mins = mins;
        addAggregate(mins);
    }

    public ArrayList<Sum> getSums() {
        return sums;
    }

    public void setSums(ArrayList<Sum> sums) {
        this.sums = sums;
        addAggregate(sums);
    }

    public ArrayList<Count> getCounts() {
        return counts;
    }

    public void setCounts(ArrayList<Count> counts) {
        this.counts = counts;
        addAggregate(counts);
    }

    public Where getWhere() {
        return where;
    }

    public void setWhere(Where where) {
        this.where = where;
    }

    public Having getHaving() {
        return having;
    }

    public void setHaving(Having having) {
        this.having = having;
    }

    private void addAggregate(ArrayList<? extends Aggregate> aggregates) {
        if (aggregates == null) return;
        if (selection == null) selection = new Selection();
        for (Aggregate aggregate : aggregates) {
            ViewField field = aggregate.getField();
            this.selection.addField(new ViewField(field.getTableName(),
                    aggregate.VIEW_TYPE + field.getColumnName(),
                    field.getFamilyName(),
                    Value.TYPE.INTEGER));
        }
    }

    public ArrayList<Aggregate> getAggregates() {
        ArrayList<Aggregate> aggregates = new ArrayList<>();
        if (maxes != null)
            aggregates.addAll(maxes);

        if (mins != null)
            aggregates.addAll(mins);

        if (sums != null)
            aggregates.addAll(sums);

        if (counts != null)
            aggregates.addAll(counts);

        return aggregates;
    }

    public void updateActualName() {
        name = "vt_";
        if (selection != null)
            name += selection.getId();
        if (maxes != null)
            for (Max max : maxes)
                name += max.getGroupByString();

        if (mins != null)
            for (Min min : mins)
                name += min.getGroupByString();

        if (sums != null)
            for (Sum sum : sums)
                name += sum.getGroupByString();

        if (counts != null)
            for (Count count : counts)
                name += count.getGroupByString();

        if (where != null && !where.isEmpty())
            name += where.getId();

        if (having != null)
            name += having.getId();
    }

    @Override
    public String toString() {
        return "View{" +
                "name='" + name + '\'' +
                ", selection=" + selection +
                ", where=" + where +
                ", having=" + having +
                '}';
    }
}
