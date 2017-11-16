package simpledb.file;

/**
 * Version 1, I use string to represent boolean type in SimpleDB.
 * <p>
 * Note: there is another file named BOOLEAN2 under the same directory,
 * which implement similar functionality using JAVA BOOLEAN type
 *
 * @author Michael
 */
public class BOOLEAN {

    private String value;

    /**
     * true  - "t"	use "t" to represent true
     * false - "f"	use "f" to represent false
     * null  -		represent null value
     */

    public BOOLEAN() {
        value = "";
    }

    public BOOLEAN(String val) {
        if (val.toLowerCase().equals("t")) {
            value = "t";
        } else if (val.toLowerCase().equals("f")) {
            value = "f";
        } else {
            value = "";
        }
    }

    public String getValue() {
        return value;
    }

    public void setValue(String[] val) {
        if (val[0].toLowerCase() == "t") {
            value = "t";
        } else if (val[0].toLowerCase() == "f") {
            value = "f";
        } else {
            value = "";
        }
    }

    public boolean isBoolean(Object obj) {
        BOOLEAN bool = (BOOLEAN) obj;
        if (bool.getValue() == "") {
            return false;
        } else {
            return true;
        }
    }
}
