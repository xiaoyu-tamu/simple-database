package simpledb.query;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class IntersectScan implements Scan {
    private Scan more, less;
    private Collection<String> fieldlist;
    private Set<String> set = new HashSet<String>();

    public IntersectScan(Scan more, Scan less, Collection<String> fields) {
        this.more = more;
        this.less = less;
        this.fieldlist = fields;
        while (less.next()) {
        		String record = new String();
        		for (String f : fields) {
        			record = record + less.getVal(f).toString();
        		}
        		if(!set.contains(record)) {
        			set.add(record);
        		}
        }
    }

    public void beforeFirst() {
        more.beforeFirst();
    }

    public boolean next() {
        while(more.next()) {
        		String record = new String();
        		for(String f: fieldlist) {
        			record = record + more.getVal(f).toString();
        		}
        		if(set.contains(record)) {
        			set.remove(record);
        			return true;
        		}
        }
        return false;
    }

    public void close() {
        more.close();
        less.close();
    }

    public Constant getVal(String fldname) {
        if (this.hasField(fldname))
            return more.getVal(fldname);
        throw new RuntimeException( "Not Exist");
    }
    public int getInt(String fldname) {
        if (this.hasField(fldname))
            return more.getInt(fldname);
        throw new RuntimeException( "Not Exist");
    }
    public String getString(String fldname) {
        if (this.hasField(fldname))
            return more.getString(fldname);
        throw new RuntimeException( "Not Exist");
    }
    public boolean hasField(String fldname) {
    		return fieldlist.contains(fldname);
    }
}
