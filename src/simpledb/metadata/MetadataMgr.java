package simpledb.metadata;

import java.util.Map;

import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class MetadataMgr {
	private static TableMgr tblmgr;
	private static ViewMgr viewmgr;
	private static StatMgr statmgr;
	private static IndexMgr idxmgr;

	public MetadataMgr(boolean isnew, Transaction tx) {
		tblmgr = new TableMgr(isnew, tx);
		viewmgr = new ViewMgr(isnew, tblmgr, tx);
		statmgr = new StatMgr(tblmgr, tx);
		idxmgr = new IndexMgr(isnew, tblmgr, tx);
	}

	public void createTable(String tblname, Schema sch, Transaction tx) {
		tblmgr.createTable(tblname, sch, tx);
	}

	public TableInfo getTableInfo(String tblname, Transaction tx) {
		return tblmgr.getTableInfo(tblname, tx);
	}

	public void createView(String viewname, String viewdef, Transaction tx) {
		viewmgr.createView(viewname, viewdef, tx);
	}

	public String getViewDef(String viewname, Transaction tx) {
		return viewmgr.getViewDef(viewname, tx);
	}

	public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
		idxmgr.createIndex(idxname, tblname, fldname, tx);
	}

	public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
		return idxmgr.getIndexInfo(tblname, tx);
	}

	public StatInfo getStatInfo(String tblname, TableInfo ti, Transaction tx) {
		return statmgr.getStatInfo(tblname, ti, tx);
	}

   /***************************************************
    * START HW 8
    ***************************************************/
	public void alterTable(String tblname, String fldname, int type, Transaction tx) {
		tblmgr.alterTable(tblname, fldname, type, tx);
	}
   /***************************************************
    * END HW 8
    ***************************************************/

}