package simpledb.tests;

import simpledb.record.RecordFile;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import static java.sql.Types.VARCHAR;


public class TestTableMgr {
    public static void main(String[] args) {
        // initialize table
        SimpleDB.init("studentdb");
        Transaction tx = new Transaction();

        TableInfo ti1 = SimpleDB.mdMgr().getTableInfo("dept", tx);
        RecordFile rf1 = new RecordFile(ti1, tx);

        while (rf1.next()) {
            System.out.println(rf1.getInt("did") + " " + rf1.getString("dname"));
        }
        rf1.close();

        // alter table to add a new column
        // according to mysql specifications that new column will have a default value

        SimpleDB.mdMgr().alterTable("dept", "newcolumn", VARCHAR, tx);

        TableInfo ti2 = SimpleDB.mdMgr().getTableInfo("dept", tx);
        RecordFile rf2 = new RecordFile(ti2, tx);
        rf2.beforeFirst();
        while (rf2.next()) {
            System.out.println(rf2.getInt("did") + " " + rf2.getString("dname") + " " + rf2.getString("newcolumn"));

        }
        rf2.close();
    }

}