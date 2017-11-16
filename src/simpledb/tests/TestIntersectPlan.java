package simpledb.tests;


import simpledb.query.IntersectPlan;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class TestIntersectPlan {
    public static void main(String[] args) {
        try {
            SimpleDB.init("studentdb");
            Transaction tx = new Transaction();
            Plan p1 = new TablePlan("student2", tx);
            Plan p2 = new TablePlan("student", tx);
            Plan p3 = new IntersectPlan(p1, p2);
            
            assert( p3.recordsOutput() == Math.min(p1.recordsOutput(), p2.recordsOutput())); 
            assert( p3.schema().fields().size() == Math.min(p1.schema().fields().size(), p2.schema().fields().size()));
            
            System.out.println("********************");
            Scan s = p3.open();
            while(s.next()) {
                System.out.println(s.getString("sname"));
            }
            System.out.println("********************");
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}