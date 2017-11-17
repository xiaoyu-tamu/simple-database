package simpledb.metadata;

import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

/**
 * The table manager.
 * There are methods to create a table, save the metadata
 * in the catalog, and obtain the metadata of a
 * previously-created table.
 *
 * @author Edward Sciore
 */
public class TableMgr {
    /**
     * The maximum number of characters in any
     * tablename or fieldname.
     * Currently, this value is 16.
     */
    public static final int MAX_NAME = 16;

    private TableInfo tcatInfo, fcatInfo;

    /**
     * Creates a new catalog manager for the database system.
     * If the database is new, then the two catalog tables
     * are created.
     *
     * @param isNew has the value true if the database is new
     * @param tx    the startup transaction
     */
    public TableMgr(boolean isNew, Transaction tx) {
        Schema tcatSchema = new Schema();
        tcatSchema.addStringField("tblname", MAX_NAME);
        tcatSchema.addIntField("reclength");
        tcatInfo = new TableInfo("tblcat", tcatSchema);

        Schema fcatSchema = new Schema();
        fcatSchema.addStringField("tblname", MAX_NAME);
        fcatSchema.addStringField("fldname", MAX_NAME);
        fcatSchema.addIntField("type");
        fcatSchema.addIntField("length");
        fcatSchema.addIntField("offset");
        fcatInfo = new TableInfo("fldcat", fcatSchema);

        if (isNew) {
            createTable("tblcat", tcatSchema, tx);
            createTable("fldcat", fcatSchema, tx);
        }
    }

    /**
     * Creates a new table having the specified name and schema.
     *
     * @param tblname the name of the new table
     * @param sch     the table's schema
     * @param tx      the transaction creating the table
     */
    public void createTable(String tblname, Schema sch, Transaction tx) {
        TableInfo ti = new TableInfo(tblname, sch);

        // insert one record into tblcat
        RecordFile tcatfile = new RecordFile(tcatInfo, tx);
        tcatfile.insert();
        tcatfile.setString("tblname", tblname);
        tcatfile.setInt("reclength", ti.recordLength());
        tcatfile.close();

        // insert a record into fldcat for each field
        RecordFile fcatfile = new RecordFile(fcatInfo, tx);
        for (String fldname : sch.fields()) {
            fcatfile.insert();
            fcatfile.setString("tblname", tblname);
            fcatfile.setString("fldname", fldname);
            fcatfile.setInt("type", sch.type(fldname));
            fcatfile.setInt("length", sch.length(fldname));
            fcatfile.setInt("offset", ti.offset(fldname));
        }
        fcatfile.close();
    }

    /***************************************************
     * START HW 8
     ***************************************************/
    public void alterTable(String tblname, String fldname, int type, Transaction tx) {
        TableInfo ti = getTableInfo(tblname, tx);
        RecordFile rf = new RecordFile(ti, tx);
        Schema sch = ti.schema();
        ArrayList<LinkedHashMap<String, DataInfo>> data = extractTableData(sch, ti, tx);

        // append new field name and data type
        for (int i = 0; i < data.size(); i++) {
            if (type == INTEGER) {
                data.get(i).put(fldname, new DataInfo(INTEGER, "0"));
            } else {
                data.get(i).put(fldname, new DataInfo(VARCHAR, "NULL"));
            }
        }

        // update schema
        if (type == INTEGER) {
            sch.addIntField(fldname);
        } else {
            sch.addStringField(fldname, 10);
        }

        // delete old table
        removeTable(tblname, tx);

        // create new table
        createTable(tblname, sch, tx);

        ti = new TableInfo(tblname, sch);
        rf = new RecordFile(ti, tx);

        // insert data into new table
        for (int i = 0; i < data.size(); i++) {
            LinkedHashMap<String, DataInfo> map = data.get(i);
            rf.insert();
            for (String fld : sch.fields()) {
                if (map.get(fld).type == INTEGER) {
                    rf.setInt(fld, Integer.parseInt(map.get(fld).value));
                } else {
                    rf.setString(fld, map.get(fld).value);
                }
            }
        }
    }

    private ArrayList<LinkedHashMap<String, DataInfo>> extractTableData(Schema sch, TableInfo ti, Transaction tx) {
        ArrayList<LinkedHashMap<String, DataInfo>> data = new ArrayList<LinkedHashMap<String, DataInfo>>();
        RecordFile rf = new RecordFile(ti, tx);

        while (rf.next()) {
            LinkedHashMap<String, DataInfo> map = new LinkedHashMap<String, DataInfo>();

            for (String fld : sch.fields()) {
                if (sch.type(fld) == INTEGER) {
                    String s = Integer.toString(rf.getInt(fld));
                    map.put(fld, new DataInfo(INTEGER, s));
                } else {
                    String s = rf.getString(fld);
                    map.put(fld, new DataInfo(VARCHAR, s));
                }
            }
            // remove record
            rf.delete();
            data.add(map);
        }
        rf.close();
        return data;
    }

    private void removeTable(String tblname, Transaction tx) {
        // detele table relative matadata
        RecordFile tcatfile = new RecordFile(SimpleDB.mdMgr().getTableInfo("tblcat", tx), tx);
        while (tcatfile.next()) {
            if (tcatfile.getString("tblname").equals(tblname)) {
                tcatfile.delete();
            }
        }
        tcatfile.close();

        // detele field relative matadata
        RecordFile fcatfile = new RecordFile(SimpleDB.mdMgr().getTableInfo("fldcat", tx), tx);
        while (fcatfile.next()) {
            if (fcatfile.getString("tblname").equals(tblname)) {
                fcatfile.delete();
            }
        }
        fcatfile.close();

        // delete physical file
        SimpleDB.fileMgr().deleteFile(tblname);
    }

    /***************************************************
     * END HW 8
     ***************************************************/


    /**
     * Retrieves the metadata for the specified table
     * out of the catalog.
     *
     * @param tblname the name of the table
     * @param tx      the transaction
     * @return the table's stored metadata
     */
    public TableInfo getTableInfo(String tblname, Transaction tx) {
        RecordFile tcatfile = new RecordFile(tcatInfo, tx);
        int reclen = -1;
        while (tcatfile.next())
            if (tcatfile.getString("tblname").equals(tblname)) {
                reclen = tcatfile.getInt("reclength");
                break;
            }
        tcatfile.close();

        RecordFile fcatfile = new RecordFile(fcatInfo, tx);
        Schema sch = new Schema();
        Map<String, Integer> offsets = new HashMap<String, Integer>();
        while (fcatfile.next())
            if (fcatfile.getString("tblname").equals(tblname)) {
                String fldname = fcatfile.getString("fldname");
                int fldtype = fcatfile.getInt("type");
                int fldlen = fcatfile.getInt("length");
                int offset = fcatfile.getInt("offset");
                offsets.put(fldname, offset);
                sch.addField(fldname, fldtype, fldlen);
            }
        fcatfile.close();
        return new TableInfo(tblname, sch, offsets, reclen);
    }

    // data structure that holds field type and actual data
    class DataInfo {
        int type;
        String value;

        public DataInfo(int type, String value) {
            this.type = type;
            this.value = value;
        }
    }
}