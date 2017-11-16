package simpledb.tests;

import simpledb.tx.*;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

//public class TestRecordMgmt {
//
//    /**
//     * @param args
//     */
//    public static void main(String[] args) {
//        Schema esch = new Schema();
//        esch.addIntField("cid");
//        esch.addStringField("title", 20);
//        esch.addIntField("deptid");
//        TableInfo eti = new TableInfo("ecourse", esch);
//        for (String fld : eti.schema().fields()) {
//            int offset = eti.offset(fld);
//            System.out.println(fld + " has offset " + offset);
//        }
//    }
//}


public class TestRecordMgmt {
	private static final int MAX_TITLE = 30;
	private static final int MAX_RECORDS = 20;//50//100//1000;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String title;
	    int index, year;
	    //create a unique directory name for test purposes
	    String date_format = "yyMMddHHmmss";
	    SimpleDateFormat sdf = new SimpleDateFormat(date_format);
	   	String dbName="trecdb"+sdf.format(new Date());//default db directory
		SimpleDB.init(dbName);
		
		//create a Schema and a table
		Schema sch = new Schema();
		sch.addIntField("movidx");
		sch.addIntField("yearmade");
		sch.addStringField("title",MAX_TITLE);
		TableInfo ti = new TableInfo("movies", sch);
		//examine record offsets
		for (String fldname : ti.schema().fields()) {
			int offset = ti.offset(fldname);
			System.out.println(fldname + " has offset " + offset);
		}
		System.out.println("Record length ="+ti.recordLength());
		
		////////////////////////////////////////////////////////////////////
		//insert data into the table
		int counter=0;
		Transaction tx = new Transaction();
		RecordFile rf = new RecordFile(ti, tx);
		try
		 {
			BufferedReader br = new BufferedReader(new FileReader("movie_titles.txt"));
			String line;
			String[] tokens;
		    while (((line = br.readLine() ) != null) && counter < MAX_RECORDS) {
			       tokens = line.split("[,\t]+");//repeatedly split on a comma or a tab
			       index = Integer.parseInt(tokens[0]);
			       year = Integer.parseInt(tokens[1]);
			       title = tokens[2];
			       if(tokens.length>3)
			    	   title += tokens[3];	//tack on part of title past a comma or tab
			       if (title.length()>MAX_TITLE)
			    	   title=title.substring(0,MAX_TITLE-1);//trim to no more than MAX_TITLE character		          
			       System.out.format("%s %s %s\n", index, year, title);
			       //insert record into movies table
			       rf.insert();
			       rf.setInt("movidx", index);
			       rf.setInt("yearmade", year);
			       rf.setString("title", title);
			       counter++;
			 }
		    br.close();
		}
		catch (IOException e )
		   {
			System.out.println("IO Exception: " + e.getMessage());
		   }
		/////////////////////////////////////////////////////////////////////
		//print file forward
		System.out.println("FORWARD listing");
		rf.beforeFirst();
		while (rf.next()) 
			System.out.println(rf.getInt("movidx")+" "+rf.getInt("yearmade")+" "+rf.getString("title"));
		
		//DELETE FROM movies WHERE yearmade < 1980;
		counter = 0;
		rf.beforeFirst();
		while (rf.next()) {
			if (rf.getInt("yearmade") < 1980) {
				counter++;
				rf.delete();
			}
		}
		System.out.println(counter+" movies made before 1980 were deleted");
		
		//print file forward
		System.out.println("FORWARD listing: note deleted records");
		rf.beforeFirst();
		while (rf.next()) 
			System.out.println(rf.getInt("movidx")+" "+rf.getInt("yearmade")+" "+rf.getString("title"));
		
		//print the file backwards
				System.out.println("BACKWARD listing");
				rf.afterLast();
				while (rf.previous()) 
					System.out.println(rf.getInt("movidx")+" "+rf.getInt("yearmade")+" "+rf.getString("title"));

		rf.close();
		tx.commit();
	}
}