package simpledb.tests;

import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class TestProductPlan {
    public static void main(String[] args) {
        try {
            SimpleDB.init("studentdb");
            Transaction tx = new Transaction();
            Plan p1 = new TablePlan("student", tx);
            Plan p2 = new TablePlan("dept", tx);
            Plan p3 = new ProductPlan(p1, p2);
            Expression lhs = new FieldNameExpression("majorid");
            Expression rhs = new FieldNameExpression("did");
            Term t = new Term(lhs, rhs);
            Predicate pred = new Predicate(t);
            Plan p4 = new SelectPlan(p3, pred);
            Scan s = p4.open();
            while (s.next()) {
                System.out.println(s.getString("sname"));
            }
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}