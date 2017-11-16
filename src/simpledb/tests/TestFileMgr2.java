package simpledb.tests;

import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

public class TestFileMgr2 {
    public static void main(String args[]) throws Exception {
        System.out.println("Start of TestFileMgr");
        SimpleDB.init("simpledbfiletest");//args[0]	test database name given directly instead of thru an argument
        String homedir = System.getProperty("user.home");
        System.out.println("home directory = " + homedir);
        FileMgr fm = SimpleDB.fileMgr();

        //fill a page with string values of the sequence of integers 0, 1, 2, ....
        Page pS = new Page();
        int offset = 0;
        int i = 0;
        while (offset + Page.STR_SIZE(Integer.toString(i).length()) < Page.BLOCK_SIZE) {
            pS.setString(offset, Integer.toString(i));
            offset += Page.INT_SIZE + Integer.toString(i).length();
            ++i;
        }
        Block bS = new Block("junkfile", 1);//filename "junkfile", block num 1
        pS.write(bS);//write the page to the designated block

        //fill a page with a sequence of ints which are multiples of INT_SIZE
        Page pI = new Page();
        for (i = 0; i * Page.INT_SIZE < Page.BLOCK_SIZE; ++i) {
            pI.setInt(i * Page.INT_SIZE, i);
        }
        Block bI = new Block("junkfile", 2);
        pI.write(bI);

        int filesize = fm.size("junkfile"); //filesize should be 3
        assert filesize == 3;

        //*******************************************************
        // Method 1 using BOOLEAN.java

        Page pB = new Page();
        int offsetBoolean = 0;
        while (offsetBoolean + Page.BOOL_SIZE() < Page.BLOCK_SIZE) {
            pB.setBoolean(offsetBoolean, "t");
            offsetBoolean += Page.BOOL_SIZE();
        }
        Block bB = new Block("junkfile", 3);
        pB.write(bB);

        filesize = fm.size("junkfile"); //filesize should be 4
        assert filesize == 4;

        // Method 2 using BOOLEAN2.java
        Page pB2 = new Page();
        int offsetBoolean2 = 0;
        while (offsetBoolean2 + Page.BOOL_SIZE() < Page.BLOCK_SIZE) {
            pB2.setBoolean(offsetBoolean2, "t");
            offsetBoolean2 += Page.BOOL_SIZE();
        }
        Block bB2 = new Block("junkfile", 4);
        pB2.write(bB2);

        filesize = fm.size("junkfile"); //filesize should be 5
        assert filesize == 5;
        //*******************************************************


        Block blk = new Block("junkfile", filesize - 1);

        //increment a designated integer
        Page p1 = new Page();
        p1.read(blk);
        int n = p1.getInt(32);//read the int at offset 32
        p1.setInt(32, n + 1);//add one to the int and rewrite it to the same offset
        p1.write(blk);

        //write a string to a designated location
        Page p2 = new Page();
        p2.setString(20, "hello");//write the string to the designated offset
        blk = p2.append("junkfile");//append the page to the file named junkfile

        Page p3 = new Page();
        p3.read(blk);
        String s = p3.getString(20);
        System.out.println("Block " + blk.number() + " contains " + s);


        //***************************************************
        System.out.println("\n*****************\nMy test cases");
        // BOOLEAN.java test case
        Page p4 = new Page();
        p4.setBoolean(5, "t");//write the string to the designated offset
        blk = p4.append("junkfile");//append the page to the file named junkfile

        Page p5 = new Page();
        p5.read(blk);
        String ss = p5.getBoolean(5).getValue();
        System.out.println("Method1 : Block " + blk.number() + " contains " + ss);

        // BOOLEAN2.java test case
        Page p6 = new Page();
        p6.setBoolean(5, "t");//write the string to the designated offset
        blk = p6.append("junkfile");//append the page to the file named junkfile

        Page p7 = new Page();
        p7.read(blk);
        String ss2 = Boolean.toString(p7.getBoolean2(5).getValue().booleanValue());
        System.out.println("Method2 : Block " + blk.number() + " contains " + ss2);
        //***************************************************

        System.out.println("end of TestFileMgr");
    }
}

