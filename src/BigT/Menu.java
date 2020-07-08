package BigT;
import btree.*;
import diskmgr.*;
import global.*;
import heap.*;
import iterator.MapUtils;
import bufmgr.*;
import static global.GlobalConst.*;

import java.io.*;
import java.util.*;

import static global.Convert.*;

public class Menu {
    static String name;
    static String filename;
    static int DBtype;
    static HashMap<String, bigT> DB_list = new HashMap<String, bigT>();
	static int num_buff = NUMBUF;
    static SystemDefs sysdefs = new SystemDefs("bigDB", INITIAL_NUM_OF_DATA_PAGES, num_buff, "Clock");

    public static int recordCount (String filename) throws IOException {
        BufferedReader br = null;
        int count = 0;
        String line = "";

        br = new BufferedReader(new FileReader(filename));
        while ((line = br.readLine()) != null)
        {
            //remove the first empty space when starting the read
            line = line.replaceFirst("^\uFEFF", "");

            count++;
        }

        return count;
    }

    public static void sortMaps(int NUMBUF, int type, String name)
    {
        sysdefs = new SystemDefs("bigDB", 0, NUMBUF, "Clock");
        bigT db;

        if(DB_list.containsKey(name))
        {
            db = DB_list.get(name);
        }
        else
        {
            System.out.println("Database " + name + " does not exist.");
            return;
        }
        System.out.println("Sorting...");
        try {
            db.fileSort(type);
        } catch (IteratorException e) {
            e.printStackTrace();
        } catch (ScanIteratorException e) {
            e.printStackTrace();
        } catch (ConstructPageException e) {
            e.printStackTrace();
        } catch (UnpinPageException e) {
            e.printStackTrace();
        } catch (PinPageException e) {
            e.printStackTrace();
        } catch (AddFileEntryException e) {
            e.printStackTrace();
        } catch (GetFileEntryException e) {
            e.printStackTrace();
        } catch (KeyNotMatchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NodeNotMatchException e) {
            e.printStackTrace();
        } catch (LeafDeleteException e) {
            e.printStackTrace();
        } catch (LeafInsertRecException e) {
            e.printStackTrace();
        } catch (IndexSearchException e) {
            e.printStackTrace();
        } catch (InsertException e) {
            e.printStackTrace();
        } catch (ConvertException e) {
            e.printStackTrace();
        } catch (DeleteRecException e) {
            e.printStackTrace();
        } catch (KeyTooLongException e) {
            e.printStackTrace();
        } catch (IndexInsertRecException e) {
            e.printStackTrace();
        }
        db.closeBigt();
    }

    public static void mapInsert(String name, int type, String RL, String CL, String VAL, int TS, int NUMBUF) throws FileIOException, DiskMgrException, InvalidPageNumberException, IOException, GetFileEntryException, ConstructPageException, AddFileEntryException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException
    {
        sysdefs = new SystemDefs("bigDB", 0, NUMBUF, "Clock");
        pcounter.initialize();
        bigT db;

        if (DB_list.containsKey(name))
        {
            db = DB_list.get(name);
        }
        else
        {
            db = new bigT(name);
            DB_list.put(name, db);
        }

        byte[] mapPtr = new byte[MAP_SIZE];

        //read each record and convert to byte map
        MID mid = new MID();
        setStrValue (RL, 0, mapPtr);
        setStrValue (CL, 30, mapPtr);
        setStrValue (VAL, 50, mapPtr);
        setIntValue (TS, 60, mapPtr);

        KeyClass r_key;
        KeyClass c_key;
        KeyClass rv_key;
        KeyClass rc_key;
        KeyClass t_key;

        //insert to data and index
        switch (type)
        {
            case 1:
                mid = db.insertMap(name, type, mapPtr);
                //used for search
                rc_key = new StringKey(RL+CL);
                if (mid != null)
                {
                    db.insertIndex(rc_key, mid, name + type +"INSERT_INDEX");
                }
                break;
            case 2:
                mid = db.insertMap(name, type, mapPtr);
                if (mid != null) {
                    r_key = new StringKey(RL);
                    db.insertIndex(r_key, mid, name + "R_INDEX");
                    //used for search
                    rc_key = new StringKey(RL+CL);
                    db.insertIndex(rc_key, mid, name + type + "INSERT_INDEX");
                }
                break;
            case 3:
                mid = db.insertMap(name, type, mapPtr);
                if (mid != null) {
                    c_key = new StringKey(CL);
                    db.insertIndex(c_key, mid, name + "C_INDEX");
                    //used for search
                    rc_key = new StringKey(RL+CL);
                    db.insertIndex(rc_key, mid, name + type + "INSERT_INDEX");
                }
                break;
            case 4:
                mid = db.insertMap(name, type, mapPtr);
                if (mid != null) {
                    rc_key = new StringKey(RL+CL);
                    t_key = new IntegerKey(TS);
                    db.insertIndex(t_key, mid, name + "T_INDEX");
                    db.insertIndex(rc_key, mid, name + "RC_INDEX");
                    db.insertIndex(rc_key, mid, name + type + "INSERT_INDEX");
                }
                break;
            case 5:
                mid = db.insertMap(name, type, mapPtr);
                if (mid != null) {
                    rv_key = new StringKey(RL+VAL);
                    t_key = new IntegerKey(TS);
                    db.insertIndex(rv_key, mid, name + "RV_INDEX");
                    db.insertIndex(t_key, mid, name + "T_INDEX");
                    //used for search
                    rc_key = new StringKey(RL+CL);
                    db.insertIndex(rc_key, mid, name + type + "INSERT_INDEX");
                }
                break;
        }

        db.closeBigt();
        if (type != 1)
        {
            sortMaps(NUMBUF, type, name);
        }
        System.out.println("Done!");
        db.getCount();
        System.out.println("Number of disk page reads: " + pcounter.getreadCount());
        System.out.println("Number of disk page writes: " + pcounter.getwriteCount());
    }

    public static void batchInsert(String name, int type, String filename, int NUMBUF) throws FileIOException, DiskMgrException, InvalidPageNumberException, IOException, GetFileEntryException, ConstructPageException, AddFileEntryException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException
    {
        sysdefs = new SystemDefs("bigDB", 0, NUMBUF, "Clock");
        pcounter.initialize();
        bigT db;

        if (DB_list.containsKey(name))
        {
            db = DB_list.get(name);
        }
        else
        {
            db = new bigT(name);
            DB_list.put(name, db);
        }

        BufferedReader br = null;
        String line = "";

        byte[] mapPtr = new byte[MAP_SIZE];
        //check if db exist

        int total_count = recordCount(filename);
        int current_count = 0;
        int current_progress = 0;
        System.out.println("Number of maps: " + total_count);
        System.out.print("Progress: 0" + "(0%) ");
        br = new BufferedReader(new FileReader(filename));
        while ((line = br.readLine()) != null)
        {
            if ((current_count * 100 / total_count) % 10 == 0)
            {
                if (current_progress != (current_count * 100 / total_count))
                {
                    current_progress = current_count * 100 / total_count;
                    System.out.print(current_count + "(" + (current_count * 100 / total_count) + "%) ");
                }
            }
        	//remove the first empty space when starting the read
			line = line.replaceFirst("^\uFEFF", "");

			//read each record and convert to byte map
            String[] data = line.split(",");
            MID mid = new MID();
            setStrValue ((String)data[0], 0, mapPtr);
            setStrValue ((String)data[1], 30, mapPtr);
            setStrValue ((String)data[2], 50, mapPtr);
            setIntValue (Integer.valueOf (data[3]), 60, mapPtr);
                        
            KeyClass r_key;
            KeyClass c_key;
            KeyClass rv_key;
            KeyClass rc_key;
            KeyClass t_key;

            //insert to data and index
            switch (type)
            {
                case 1:
                    mid = db.insertMap(name, type, mapPtr);
                    //used for search
					rc_key = new StringKey((String)data[0] + (String)data[1]);
					if (mid != null)
                    {
                        db.insertIndex(rc_key, mid, name + type + "INSERT_INDEX");
                    }
                    break;
                case 2:
                    mid = db.insertMap(name, type, mapPtr);
                    if (mid != null) {
                        r_key = new StringKey((String) data[0]);
                        db.insertIndex(r_key, mid, name + "R_INDEX");
                        //used for search
                        rc_key = new StringKey((String) data[0] + (String) data[1]);
                        db.insertIndex(rc_key, mid, name + type + "INSERT_INDEX");
                    }
                    break;
                case 3:
                    mid = db.insertMap(name, type, mapPtr);
                    if (mid != null) {
                        c_key = new StringKey((String) data[1]);
                        db.insertIndex(c_key, mid, name + "C_INDEX");
                        //used for search
                        rc_key = new StringKey((String) data[0] + (String) data[1]);
                        db.insertIndex(rc_key, mid, name + type + "INSERT_INDEX");
                    }
                    break;
                case 4:
                    mid = db.insertMap(name, type, mapPtr);
                    if (mid != null) {
                        rc_key = new StringKey((String) data[0] + (String) data[1]);
                        t_key = new IntegerKey(Integer.valueOf(data[3]));
                        db.insertIndex(t_key, mid, name + "T_INDEX");
                        db.insertIndex(rc_key, mid, name + "RC_INDEX");
                        db.insertIndex(rc_key, mid, name + type + "INSERT_INDEX");
                    }
                    break;
                case 5:
                    mid = db.insertMap(name, type, mapPtr);
                    if (mid != null) {
                        rv_key = new StringKey((String) data[0] + (String) data[2]);
                        t_key = new IntegerKey(Integer.valueOf(data[3]));
                        db.insertIndex(rv_key, mid, name + "RV_INDEX");
                        db.insertIndex(t_key, mid, name + "T_INDEX");
                        //used for search
                        rc_key = new StringKey((String) data[0] + (String) data[1]);
                        db.insertIndex(rc_key, mid, name + type + "INSERT_INDEX");
                    }
                    break;
            }

            current_count ++;
        }

        System.out.println(current_count + "(100.0%)");

        br.close();

        db.closeBigt();
        if (type != 1)
        {
            sortMaps(NUMBUF, type, name);
        }
        System.out.println("Done!");
        db.getCount();
        System.out.println("Number of disk page reads: " + pcounter.getreadCount());
        System.out.println("Number of disk page writes: " + pcounter.getwriteCount());
    }

	public static void query(String name, int ordertype, String rowFilter, String columnFilter, String valueFilter, int NUMBUF)
	{
        sysdefs = new SystemDefs("bigDB", 0, NUMBUF, "Clock");
        pcounter.initialize();
        bigT db;

        if(DB_list.containsKey(name))
        {
            db = DB_list.get(name);
        }
        else
        {
            System.out.println("Database " + name + " does not exist.");
            return;
        }
        for(int index_type: db.type_used)
        {
            System.out.println("Querying file type " + index_type + "...");
        }
        System.out.println("DB name: " + name);
        Stream st = db.openStream(db, name, ordertype, rowFilter, columnFilter, valueFilter, NUMBUF);
		st.getNext();
        System.out.println("Number of disk page reads: " + pcounter.getreadCount());
		System.out.println("Number of disk page writes: " + pcounter.getwriteCount());
        db.closeBigt();
    }

    public static void rowJoin(String btname1, String btname2, String outbtname, String ColumnName, int NUMBUF) throws IOException, GetFileEntryException, ConstructPageException, AddFileEntryException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, ScanIteratorException, FileIOException, DiskMgrException, InvalidPageNumberException
    {
        //initialize variables for table creation and retrieval
        sysdefs = new SystemDefs("bigDB", 0, NUMBUF, "Clock");
        pcounter.initialize();
        boolean insert = false;
        //out table
        bigT db_out;

        //left table
        bigT db1;

        //right table
        bigT db2;

        //out table
        if (DB_list.containsKey(outbtname))
        {
            db_out = DB_list.get(outbtname);
        }
        else
        {
            db_out = new bigT(outbtname);
            DB_list.put(outbtname, db_out);
        }

        ArrayList<MAP> leftArray = new ArrayList<MAP>();
        ArrayList<MAP> rightArray = new ArrayList<MAP>();
        byte[] mapPtr = new byte[MAP_SIZE];
        MID mid = new MID();
        KeyClass rc_key;

        if(DB_list.containsKey(btname1))
        {
            db1 = DB_list.get(btname1);
        }
        else
        {
            System.out.println("Database " + btname1 + " does not exist.");
            return;
        }

        if(DB_list.containsKey(btname2))
        {
            db2 = DB_list.get(btname2);
        }
        else
        {
            System.out.println("Database " + btname2 + " does not exist.");
            return;
        }

        Stream leftStream = db1.openStream(db1, btname1, 2, "*", ColumnName, "*", NUMBUF);
        leftArray = leftStream.getArray();

        Stream rightStream = db2.openStream(db2, btname2, 2, "*", ColumnName, "*", NUMBUF);
        rightArray = rightStream.getArray();

    	/*
    	System.out.println("Printing Left Stuff");
    	for(MAP m : leftArray) {
    		System.out.println(m.getRowLabel());
    		System.out.println(m.getColumnLabel());
    	}
    	
    	System.out.println("Printing Right Stuff");
    	for(MAP m : rightArray) {
    		System.out.println(m.getRowLabel());
    		System.out.println(m.getColumnLabel());
    	}
		*/
    	
    	// Getting the most recent values for each row in the left table
    	ArrayList<String[]> leftValArrays = new ArrayList<String[]>();
    	ArrayList<String> leftRows = new ArrayList<String>();
    	MAP currentMAPLeft = leftArray.get(0);
    	String[] initLeftArray = {currentMAPLeft.getRowLabel(), currentMAPLeft.getValue(), Integer.toString(currentMAPLeft.getTimeStamp())};
    	leftValArrays.add(initLeftArray);
    	leftRows.add(currentMAPLeft.getRowLabel());
    	
    	for(int i = 1; i < leftArray.size(); i++) {
    		MAP nextMAPLeft = leftArray.get(i);
    		if(leftRows.contains(nextMAPLeft.getRowLabel())) {
    			int ind = leftRows.indexOf(nextMAPLeft.getRowLabel());
    			String[] current = leftValArrays.get(ind);
    			if(Integer.parseInt(current[2]) < nextMAPLeft.getTimeStamp()) {
    				String[] replacement = {nextMAPLeft.getRowLabel(), nextMAPLeft.getValue(), Integer.toString(nextMAPLeft.getTimeStamp())};
    				leftValArrays.set(ind, replacement);
    			}
    		}
    		else {
    			leftRows.add(nextMAPLeft.getRowLabel());
    			String[] newLeftVal = {nextMAPLeft.getRowLabel(), nextMAPLeft.getValue(), Integer.toString(nextMAPLeft.getTimeStamp())};
    			leftValArrays.add(newLeftVal);
    		}

    	}
    	
    	// Getting the most recent values for each row in the right table
    	ArrayList<String[]> rightValArrays = new ArrayList<String[]>();
    	ArrayList<String> rightRows = new ArrayList<String>();
    	MAP currentMAPRight = rightArray.get(0);
    	String[] initRightArray = {currentMAPRight.getRowLabel(), currentMAPRight.getValue(), Integer.toString(currentMAPRight.getTimeStamp())};
    	rightValArrays.add(initRightArray);
    	rightRows.add(currentMAPRight.getRowLabel());
    	
    	for(int i = 1; i < rightArray.size(); i++) {
    		MAP nextMAPRight = rightArray.get(i);
    		if(rightRows.contains(nextMAPRight.getRowLabel())) {
    			int ind = rightRows.indexOf(nextMAPRight.getRowLabel());
    			String[] current = rightValArrays.get(ind);
    			if(Integer.parseInt(current[2]) < nextMAPRight.getTimeStamp()) {
    				String[] replacement = {nextMAPRight.getRowLabel(), nextMAPRight.getValue(), Integer.toString(nextMAPRight.getTimeStamp())};
    				rightValArrays.set(ind, replacement);
    			}
    		}
    		else {
    			rightRows.add(nextMAPRight.getRowLabel());
    			String[] newRightVal = {nextMAPRight.getRowLabel(), nextMAPRight.getValue(), Integer.toString(nextMAPRight.getTimeStamp())};
    			rightValArrays.add(newRightVal);
    		}

    	}
    	
    	// Iterating through the most recent values for each row in the left and right tables to find the matches
    	for(String[] sL : leftValArrays) {
    		//System.out.println(sL[0] + " " + sL[1] + " " + sL[2]);
    		for(String[] sR : rightValArrays) {
    			// If the most recent value of the current left row and current right row match
    			if(sL[1].equals(sR[1])) {
                	String newRow = sL[0] + ":" + sR[0];

                	// If the combined label is too long, take the first 20 chars; this shouldn't be an issue for the demo,
                	// but should make it easier to test without having to modify the test CSVs
                	if (newRow.length() > 20) {
                		newRow = newRow.substring(0, 20);
                	}
                	//System.out.println("Joined row label: " + newRow);
                	
                	// I think this is the only thing that still needs to be fixed
                	// Known issues with the current method:
                	//	1) Need to find a way to only get the MAPs with the matching row from the left table of the join for leftTablesL/ same for rightTablesR
                	//		Currently, I think this approach is just getting all of the matching MAPs from every part of db instead
                	//	2) After the first set of joined maps are inserted, the next iteration when trying to run this line results in an error:
                		/*
							btree.ConstructPageException: pinpage failed
								at btree.BTreeHeaderPage.<init>(BTreeHeaderPage.java:134)
								at btree.BTreeFile.<init>(BTreeFile.java:226)
								at BigT.bigT.scanForKey(bigT.java:546)
								at BigT.Stream.query(Stream.java:147)
								at BigT.Menu.RowJoin(Menu.java:461)
								at BigT.Menu.rowJoin(Menu.java:350)
								at BigT.Menu.main(Menu.java:844)
	                	*/
                	//query the maps with row label sL[0] in the left table
                    ArrayList<MAP> leftTablesL = leftStream.query(db1, sL[0], "*", "*", 1);

                    //query the maps with row label sR[0] in the right table
                	ArrayList<MAP> rightTablesR = rightStream.query(db2, sR[0], "*", "*", 1);
                	
                	//Identify the columns that the two rows have in common that are NOT the column on which they're joined
                	ArrayList<String> sharedCols = new ArrayList<String>();
                	
                	for(int i = 0; i < leftTablesL.size(); i++)
                	{
                		for(int j = 0; j < rightTablesR.size(); j++)
                		{
                			String leftColumn = leftTablesL.get(i).getColumnLabel();
                			String rightColumn = rightTablesR.get(j).getColumnLabel();
                			
                			//check if the columns equal each other, and that the column is not the joined one
                			if(leftColumn.equals(rightColumn) && !leftColumn.equals(ColumnName))
                			{
                				sharedCols.add(leftColumn);
                			}
                		}
                	}

                    if (!insert)
                    {
                        System.out.println("Inserting joined maps to " + outbtname + "...");
                        insert = true;
                    }
                    // Iterate through the MAPs and add them to the new output table
                    for(int i = 0; i < leftTablesL.size(); i++)
                	{
                		// Insert MAP from left table
                		MAP leftMAP = leftTablesL.get(i);
                		// If the MAP has a shared column that is not the joined column, modify its column label to indicate
                		// that it is from the left row
                		if(sharedCols.contains(leftMAP.getColumnLabel())) {
                			// Generate new column label, and shorten it if necessary
                			String newCol = leftMAP.getColumnLabel() + "_L";
                			if(newCol.length() > 20) {
                				newCol = newCol.substring(0, 18) + "_L";
                			}
                			//System.out.println("Inserting MAP:" + newRow + " " + newCol + " " + leftMAP.getValue() + " " + leftMAP.getTimeStamp());
                			//mapInsert(outbtname, 1, newRow, newCol, leftMAP.getValue(), leftMAP.getTimeStamp(), NUMBUF);
                            setStrValue (newRow, 0, mapPtr);
                            setStrValue (newCol, 30, mapPtr);
                            setStrValue (leftMAP.getValue(), 50, mapPtr);
                            setIntValue (leftMAP.getTimeStamp(), 60, mapPtr);

                            mid = db_out.insertMap(outbtname, 1, mapPtr);
                            //used for search
                            rc_key = new StringKey(newRow+newCol);
                            if (mid != null)
                            {
                                db_out.insertIndex(rc_key, mid, outbtname + 1 +"INSERT_INDEX");
                            }
                		}
                		// Otherwise, can just insert as normal I think. 
                		// Don't need to do anything special for MAPs with unique columns, and catching more than three inserted MAPs
                		// for the join column should be handled automatically by the map insertion
                		else {
                			//System.out.println("Inserting MAP:" + newRow + " " + leftMAP.getColumnLabel() + " " + leftMAP.getValue() + " " + leftMAP.getTimeStamp());
                			//mapInsert(outbtname, 1, newRow, leftMAP.getColumnLabel(), leftMAP.getValue(), leftMAP.getTimeStamp(), NUMBUF);
                            setStrValue (newRow, 0, mapPtr);
                            setStrValue (leftMAP.getColumnLabel(), 30, mapPtr);
                            setStrValue (leftMAP.getValue(), 50, mapPtr);
                            setIntValue (leftMAP.getTimeStamp(), 60, mapPtr);

                            mid = db_out.insertMap(outbtname, 1, mapPtr);
                            //used for search
                            rc_key = new StringKey(newRow+leftMAP.getColumnLabel());
                            if (mid != null)
                            {
                                db_out.insertIndex(rc_key, mid, outbtname + 1 +"INSERT_INDEX");
                            }
                		}
                	}
                    for(int i = 0; i < rightTablesR.size(); i++) {
                		// Insert MAP from right table
                		MAP rightMAP = rightTablesR.get(i);
                		// If the MAP has a shared column that is not the joined column, modify its column label to indicate
                		// that it is from the right row
                		if(sharedCols.contains(rightMAP.getColumnLabel())) {
                			// Generate new column label, and shorten it if necessary
                			String newCol = rightMAP.getColumnLabel() + "_R";
                			if(newCol.length() > 20) {
                				newCol = newCol.substring(0, 18) + "_R";
                			}
                			//System.out.println("Inserting MAP:" + newRow + " " + newCol + " " + rightMAP.getValue() + " " + rightMAP.getTimeStamp());
                			//mapInsert(outbtname, 1, newRow, newCol, rightMAP.getValue(), rightMAP.getTimeStamp(), NUMBUF);
                            setStrValue (newRow, 0, mapPtr);
                            setStrValue (newCol, 30, mapPtr);
                            setStrValue (rightMAP.getValue(), 50, mapPtr);
                            setIntValue (rightMAP.getTimeStamp(), 60, mapPtr);

                            mid = db_out.insertMap(outbtname, 1, mapPtr);
                            //used for search
                            rc_key = new StringKey(newRow+newCol);
                            if (mid != null)
                            {
                                db_out.insertIndex(rc_key, mid, outbtname + 1 +"INSERT_INDEX");
                            }
                		}
                		// Otherwise, can just insert as normal I think. 
                		// Don't need to do anything special for MAPs with unique columns, and catching more than three inserted MAPs
                		// for the join column should be handled automatically by the map insertion
                		else {
                			//System.out.println("Inserting MAP:" + newRow + " " + rightMAP.getColumnLabel());
                			//mapInsert(outbtname, 1, newRow, rightMAP.getColumnLabel(), rightMAP.getValue(), rightMAP.getTimeStamp(), NUMBUF);
                            setStrValue (newRow, 0, mapPtr);
                            setStrValue (rightMAP.getColumnLabel(), 30, mapPtr);
                            setStrValue (rightMAP.getValue(), 50, mapPtr);
                            setIntValue (rightMAP.getTimeStamp(), 60, mapPtr);

                            mid = db_out.insertMap(outbtname, 1, mapPtr);
                            //used for search
                            rc_key = new StringKey(newRow+rightMAP.getColumnLabel());
                            if (mid != null)
                            {
                                db_out.insertIndex(rc_key, mid, outbtname + 1 +"INSERT_INDEX");
                            }
                		}
                    }
                	
                	/*
                	// Iterate through the MAPs in the right table and add them to the new output table
            		for(int j = 0; j < rightTablesR.size(); j++)
            		{
                		MAP rightMAP = rightTablesR.get(j);
                		// If the MAP has a shared column that is not the joined column, modify its column label to indicate
                		// that it is from the right row
                		if(sharedCols.contains(rightMAP.getColumnLabel())) {
                			// Generate new column label, and shorten it if necessary
                			String newCol = rightMAP.getColumnLabel() + "_R";
                			if(newCol.length() > 20) {
                				newCol = newCol.substring(0, 18) + "_R";
                			}
                			System.out.println("Inserting MAP:" + newRow + " " + newCol + " " + rightMAP.getValue() + " " + rightMAP.getTimeStamp());
                			mapInsert(outbtname, 1, newRow, newCol, rightMAP.getValue(), rightMAP.getTimeStamp(), NUMBUF);
                		}
                		// Otherwise, can just insert as normal I think. 
                		// Don't need to do anything special for MAPs with unique columns, and catching more than three inserted MAPs
                		// for the join column should be handled automatically by the map insertion
                		else {
                			System.out.println("Inserting MAP:" + newRow + " " + rightMAP.getColumnLabel());
                			mapInsert(outbtname, 1, newRow, rightMAP.getColumnLabel(), rightMAP.getValue(), rightMAP.getTimeStamp(), NUMBUF);
                		}
            		}
            		*/
                			/*
            				newRow = leftTablesL.get(i).getRowLabel() + ":" + rightTablesR.get(j).getRowLabel();
            				if (newRow.length() > 20) {
                        		newRow = newRow.substring(0, 20);
                        	}
            				System.out.println("Joined row label: " + newRow);
            				
            				//insert three of the most recent maps
            				//use search from bigT?
            				byte[] mapPtrL = leftTablesL.get(i).getMapByteArray();
            				byte[] mapPtrR = rightTablesR.get(j).getMapByteArray();
            				Map<MID, MAP> search_left = db.search(mapPtrL);
            				Map<MID, MAP> search_right = db.search(mapPtrR);
            				
            				MAP max0 = search_right.get(0);
            				MAP max1 = search_right.get(1);
            				MAP max2 = search_right.get(2);
            				
            				//compare the left table to check for larger timestamps
            				for(int l = 0; l < 3; l++)
            				{
            					if(max0.getTimeStamp() < search_right.get(l).getTimeStamp())
            					{
            						max2 = max1;
            						max1 = max0;
            						max0 = search_right.get(l);
            					}
            					else if(max1.getTimeStamp() < search_right.get(l).getTimeStamp())
            					{
            						max2 = max1;
            						max1 = search_right.get(l);
            					}
            					else if(max2.getTimeStamp() < search_right.get(l).getTimeStamp())
            					{
            						max2 = search_right.get(l);
            					}
            				}
            				
            				
            				//compare the right table to check for larger timestamps
            				for(int r = 0; r < 3; r++)
            				{
            					if(max0.getTimeStamp() < search_right.get(r).getTimeStamp())
            					{
            						max2 = max1;
            						max1 = max0;
            						max0 = search_right.get(r);
            					}
            					else if(max1.getTimeStamp() < search_right.get(r).getTimeStamp())
            					{
            						max2 = max1;
            						max1 = search_right.get(r);
            					}
            					else if(max2.getTimeStamp() < search_right.get(r).getTimeStamp())
            					{
            						max2 = search_right.get(r);
            					}
            				}
            				
            				mapInsert(outbtname, 1, newRow, max0.getColumnLabel(), max0.getValue(), max0.getTimeStamp(), NUMBUF);
            				mapInsert(outbtname, 1, newRow, max1.getColumnLabel(), max1.getValue(), max1.getTimeStamp(), NUMBUF);
            				mapInsert(outbtname, 1, newRow, max2.getColumnLabel(), max2.getValue(), max2.getTimeStamp(), NUMBUF);
            				*/
                	//	}
                	//}
                	
                	/* Pseudocode for next steps:
                	 * 	Get all maps from the left table with row label sL[0]
                	 * 	Get all maps from the right table with row label sR[0]
                	 * 	Identify the columns that the two rows have in common that are NOT the column on which they're joined
                	 * 	Add these maps to a new table/stream with the new row label
                	 * 		When adding the MAPs with the join column value:
                	 * 			Need to check that no more than three are inserted
                	 * 			Keep only the three most recent, regardless of which table/row they come from
                	 *		
                	 *		When adding MAPs with a column value that both rows have:
                	 *			Change the column value of the MAP that is inserted to ColumnName_Left if from the left row/table and ColumnName_Right if from the right row/table
                	 *			(e.g. if the rowjoin is done on "Isogomphodon" but both rows also have a column named "Fox", the new MAPs to insert would be:
                	 *				R1:R2 Fox_Left V1 T1 when the MAP is from R1
                	 *				R1:R2 Fox_Right V2 T2 when the MAP is from R2
                	 *			)
                	 *
                	 *		When adding any other MAPs (with column values that are unique to the row):
                	 *			Only need to change the row label before inserting; everything else will stay the same I think
                	 *		
                	 */
                	
                	
                	
    			}
    		}
    	}

    	/*
    	for(String[] s : valArrays) {
    		System.out.println(s[0]);
    		System.out.println(s[1]);
    	}

    	*/
        System.out.println("Done!");
        db_out.getCount();
        System.out.println("Number of disk page reads: " + pcounter.getreadCount());
        System.out.println("Number of disk page writes: " + pcounter.getwriteCount());
        db_out.closeBigt();
    }

    public static void rowSort(String name, String outbtname, String ColumnName, int NUMBUF) throws IOException, ConstructPageException, GetFileEntryException, AddFileEntryException, IteratorException, NodeNotMatchException, UnpinPageException, LeafInsertRecException, IndexSearchException, InsertException, PinPageException, ConvertException, DeleteRecException, KeyNotMatchException, LeafDeleteException, KeyTooLongException, IndexInsertRecException {
        sysdefs = new SystemDefs("bigDB", 0, NUMBUF, "Clock");
        pcounter.initialize();
        byte[] mapPtr = new byte[MAP_SIZE];
        //output table
        bigT db_out;

        //input table
        bigT db_in;

        MID mid = new MID();
        KeyClass rc_key;

        if(DB_list.containsKey(outbtname))
        {
            db_out = DB_list.get(outbtname);
        }
        else
        {
            db_out = new bigT(outbtname);
            DB_list.put(outbtname, db_out);
        }

        if(DB_list.containsKey(name))
        {
            db_in = DB_list.get(name);
        }
        else
        {
            System.out.println("Database " + name + " does not exist.");
            return;
        }

        Stream st = db_in.openStream(db_in, name, 1, "*", "*", "*", NUMBUF);

        ArrayList<MAP> query = st.getArray();
        ArrayList<MAP> row = new ArrayList<MAP>();
        ArrayList<MAP> total = new ArrayList<MAP>();
        List<Integer> V = new ArrayList<Integer>();

        for (int i = 0; i < st.getArray().size(); i++)
        {
            if (query.get(i).getColumnLabel().equals(ColumnName))
            {
                row.add(query.get(i));
            }
            else
            {
                total.add(query.get(i));
            }
        }

        HashMap <String, Stack<MAP> > mapStack = new HashMap<>();

        for (int i=0; i<row.size();i++) {
            if(mapStack.containsKey(row.get(i).getRowLabel())) {
                mapStack.get(row.get(i).getRowLabel()).push(row.get(i));
            }

            else {
                Stack<MAP> s = new Stack<MAP>();
                s.push(row.get(i));
                mapStack.put(row.get(i).getRowLabel(),s);
            }
        }

        for(String key: mapStack.keySet()) {
            int Value = Integer.parseInt(mapStack.get(key).peek().getValue());
            V.add(Value);
        }

        Collections.sort(V);

        //Remove duplicate element and save to V1
        List<Integer> V1 = new ArrayList<Integer>();

        for (int i = 0;i<V.size();i++) {

            if (!V1.contains(V.get(i))) {

                V1.add(V.get(i));
            }
        }

        for(int i=0; i<V1.size();i++ ) {
            for (Stack<MAP> value : mapStack.values()) {
                int last = Integer.parseInt(value.get(value.size() - 1).getValue());
                if (last==V1.get(i)) {
                    total.addAll(value);

                }
            }
        }

        System.out.println("Inserting sorted maps to " + outbtname + "...");
        for (int i = 0; i < total.size(); i++)
        {
            setStrValue (total.get(i).getRowLabel(), 0, mapPtr);
            setStrValue (total.get(i).getColumnLabel(), 30, mapPtr);
            setStrValue (total.get(i).getValue(), 50, mapPtr);
            setIntValue (total.get(i).getTimeStamp(), 60, mapPtr);

            mid = db_out.insertMap(outbtname, 1, mapPtr);
            //used for search
            rc_key = new StringKey(total.get(i).getRowLabel() + total.get(i).getColumnLabel());
            if (mid != null)
            {
                db_in.insertIndex(rc_key, mid, outbtname + 1 +"INSERT_INDEX");
            }
        }

        System.out.println("Done!");
        db_out.getCount();
        System.out.println("Number of disk page reads: " + pcounter.getreadCount());
        System.out.println("Number of disk page writes: " + pcounter.getwriteCount());
        db_in.closeBigt();
    }

	// query dbtest 2 1 * * * 10
    // query dbtest 1 1 Hawaii Lamna * 10
    //batchinsert C:\Users\alexs\Documents\Grad_hw\CSE510\CSE510-master\CSE510\Dataset\Data1.csv 2 db 50
    //query db 2 2 [Pennsylvania, Zimbabwe] * * 50
    //batchinsert /Users/matthewbao/Documents/GitHub/CSE510/src/BigT/test.csv 2 dbtest 50
 	//batchinsert /Users/matthewbao/Documents/GitHub/test/test2.csv 2 dbtest
    //batchinsert /Users/hangzhao/Documents/GitHub/CSE510/src/BigT/test.csv 1 dbtest 50
    //batchinsert /Users/dongziming/git/CSE510/src/BigT/test.csv 1 dbtest
    //query dbtest 1 Dominica Zebra * 50
    //rowsort dbtest dbsorted Zebra 50
    //mapinsert Dominica Zebra 1 1 dbtest 1 50
	//query dbtest 2 1 Ghana Mouse 76617 50
    //format [rowlabel, columlabel, timestamp, value]
    public static void main(String[] args) throws NumberFormatException, InvalidPageNumberException, FileIOException, DiskMgrException, IOException, GetFileEntryException, PinPageException, ConstructPageException, KeyNotMatchException, IteratorException, UnpinPageException, ScanIteratorException, AddFileEntryException, KeyTooLongException, LeafInsertRecException, IndexInsertRecException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, LeafDeleteException, InsertException
    {
        String sel = "";
        
        // selection menu
        while(!sel.equals("exit")){
            System.out.println();
            System.out.println("************************* BIG TABLE OPTIONS *************************");
			System.out.println("Commands");
			System.out.println("1. Batch Insert: batchinsert DataFile IndexType DB_Name NumBuff");
            System.out.println("2. Map Insert: mapinsert RowLabel ColumnLabel Value TimeStamp DB_Name IndexType NumBuff");
            System.out.println("3. Query: query DB_Name OrderType RowFilter ColumnFilter ValueFilter NumBuff");
			System.out.println("	Usage: ");
			System.out.println("	[x,x]: for range search");
			System.out.println("	*: to leave the filter empty (select all)");
            System.out.println("4. Row Join: rowjoin DB_Name1 DB_Name2 OutputDBName ColumnFilter NumBuff");
            System.out.println("5. Row Sort: rowsort InputDBName OutputDBName ColumnLabel NumBuff");
            System.out.println("6. Get Counts: getcount");
            System.out.println("7. Delete Database: deletedb");
			System.out.println("8. Exit BigDB: exit");
            System.out.println("Command: ");

            Scanner sc = new Scanner(System.in);
            sel = sc.nextLine();
            String[] command = sel.split(" ");
            long startTime;
            long endTime = 0;

            switch(command[0])
            {
                case "batchinsert":
                    System.out.println("Starting Batch Insert");
                    System.out.println("Inserting maps to " + command[3] + "...");
                    try {
                        startTime = System.currentTimeMillis();
                        batchInsert(command[3], Integer.parseInt(command[2]), command[1], Integer.parseInt(command[4]));
                        endTime = System.currentTimeMillis();
						System.out.println("This process took: " + (endTime - startTime) + " milliseconds");
                    } catch (FileIOException e) {
                        e.printStackTrace();
                    } catch (DiskMgrException e) {
                        e.printStackTrace();
                    } catch (InvalidPageNumberException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case "mapinsert":
                    System.out.println("Starting Map Insert");
                    System.out.println("Inserting map to " + command[5] + "...");
                    try {
                        startTime = System.currentTimeMillis();
                        mapInsert(command[5], Integer.parseInt(command[6]), command[1], command[2], command[3], Integer.parseInt(command[4]), Integer.parseInt(command[7]));
                        endTime = System.currentTimeMillis();
                        System.out.println("This process took: " + (endTime - startTime) + " milliseconds");
                    } catch (FileIOException e) {
                        e.printStackTrace();
                    } catch (DiskMgrException e) {
                        e.printStackTrace();
                    } catch (InvalidPageNumberException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case "query":
                    System.out.println("Starting Query");
                    if(command.length != 7){
                        System.out.println("Query invalid");
                        break;
                    }
                    //make an array that holds only 7 strings
                    String[] str = new String[7];
                    int strNum = 0;
                    try{
                        for(int i = 1; i < command.length; i++)
                        {
                    	   //store two strings into the array if they are a part of the same range
                    	   if(command[i].charAt(0) == '[' && command[i].charAt(command[i].length() - 1) != ']')
                    	   {
                    		  str[strNum] = command[i] + command[i+1];
                    		  i++;
                    	   }
                    	   else
                    	   {
                    		  str[strNum] = command[i];
                    	   }
                    	   strNum++;
                        }
                    }catch(Exception e){
                        System.out.println("Query invalid");
                        break;
                    }
                    //input the array as parameters
                    startTime = System.currentTimeMillis();
					query(str[0], Integer.parseInt(str[1]), str[2], str[3], str[4], Integer.parseInt(str[5]));
					endTime = System.currentTimeMillis();
					System.out.println("This process took: " + (endTime - startTime) + " milliseconds");
                    break;
                case "rowjoin":
                    if(command.length != 6){
                        System.out.println("Command invalid");
                        break;
                    }
                    startTime = System.currentTimeMillis();
                    try {
                        System.out.println("Starting Row Join");
                        System.out.println("Joining tables " + command[1] + " and " + command[2] + "...");
                        rowJoin(command[1], command[2], command[3], command[4], Integer.parseInt(command[5]));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    endTime = System.currentTimeMillis();
                    System.out.println("This process took: " + (endTime - startTime) + " milliseconds");
                    break;
                case "rowsort":
                    if(command.length != 5){
                        System.out.println("Command invalid");
                        break;
                    }
                    startTime = System.currentTimeMillis();
                    try {
                        System.out.println("Starting Row Sort");
                        System.out.println("Sorting table " + command[1] + "...");
                        rowSort(command[1], command[2], command[3], Integer.parseInt(command[4]));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    endTime = System.currentTimeMillis();
                    System.out.println("This process took: " + (endTime - startTime) + " milliseconds");
                    break;
                case "getcount":
                    for(String dbs: DB_list.keySet())
                    {
                        DB_list.get(dbs).getCount();
                    }
                    break;
				case "deletedb":
					System.out.println("Deleting bigDB");
                    for(String dbs: DB_list.keySet())
                    {
                        DB_list.get(dbs).deleteBigt();
                    }
					break;
				case "exit":
                    System.out.println("Closing All DBs");
					System.exit(0);
                    break;
                default:
					System.out.println("Not a valid command");
                    break;
            }
        }
    }
}
