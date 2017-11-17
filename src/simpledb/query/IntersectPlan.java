package simpledb.query;

import simpledb.record.Schema;

public class IntersectPlan implements Plan {
    private Plan p1, p2;
    private Schema schema = new Schema();

    public IntersectPlan(Plan p1, Plan p2) {
        this.p1 = p1;
        this.p2 = p2;
        this.schema = p1.schema();

    }

    public Scan open() {
        Scan more = p1.open();
        Scan less = p2.open();
        if (p1.recordsOutput() < p2.recordsOutput()) {
            more = p2.open();
            less = p1.open();
        }
        return new IntersectScan(more, less, schema.fields());
    }

    public int blocksAccessed() {
        return p1.blocksAccessed() + p2.blocksAccessed();
    }

    public int recordsOutput() {
        return Math.min(p1.recordsOutput(), p2.recordsOutput());
    }

    public int distinctValues(String fldname) {
        return p1.distinctValues(fldname) + p2.distinctValues(fldname);
    }

    public Schema schema() {
        return schema;
    }
}
