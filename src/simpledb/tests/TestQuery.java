package simpledb.tests;
import java.sql.*;
import java.util.*;
import simpledb.query.*;
import java.sql.Connection;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class TestQuery {
	public static void main(String[] args) {
		Connection conn = null;
		try {
			SimpleDB.init("studentdb");
			Transaction tx = new Transaction();
			Plan pstudent = new TablePlan("student", tx);
			Plan pdept = new TablePlan("dept", tx);
			Plan penroll = new TablePlan("enroll", tx);
			Plan psection = new TablePlan("section", tx);
			
			ProductPlan pds = new ProductPlan(pstudent,pdept);
			ProductPlan pdse = new ProductPlan(pds,penroll);
			ProductPlan pdses = new ProductPlan(pdse,psection);
			
			Expression lhs1 = new FieldNameExpression("sid");
			Expression rhs1 = new FieldNameExpression("studentid");
			Term t1 = new Term (lhs1,rhs1);
			
			Expression lhs2 = new FieldNameExpression("sectid");
			Expression rhs2 = new FieldNameExpression("sectionid");
			Term t2 = new Term (lhs2,rhs2);
			
			Expression lhs3 = new FieldNameExpression("did");
			Expression rhs3 = new FieldNameExpression("majorid");
			Term t3 = new Term (lhs3,rhs3);
			
			Predicate pred = new Predicate(t1);
			pred.conjoinWith(new Predicate(t2));
			pred.conjoinWith(new Predicate(t3));
			
			Plan p1 = new SelectPlan(pdses, pred);
			Collection<String> c = Arrays.asList("sname", "dname", "grade");
			Plan p2 = new ProjectPlan(p1,c);
			Scan s = p2.open();
			s.beforeFirst();
			
			while(s.next()) {
				for (String sn:c) {
					System.out.print(s.getString(sn) + "\t");
				}
				System.out.print("\n");
			}
			
			s.close();		
		}
		finally{
			try {
				if (conn != null)
					conn.close();
			}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
	}
}