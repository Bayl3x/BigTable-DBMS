package BigT;

import java.io.IOException;
import java.util.*;
import global.*;
import btree.*;
import heap.*;
import static global.GlobalConst.*;

//This class will be similar to heap.Scan, however, will provide different types of accesses to the bigtable
public class Stream
{
	bigT db;
	ArrayList<MAP> query_map;

	public Stream(bigT db, String name, int ordertype, String rowFilter, String columnFilter, String valueFilter, int numbuf) {
		//check if the big table exists
		query_map = new ArrayList<>();

		try {
			query_map = query(db, rowFilter, columnFilter, valueFilter, ordertype);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (GetFileEntryException e) {
			e.printStackTrace();
		} catch (PinPageException e) {
			e.printStackTrace();
		} catch (ConstructPageException e) {
			e.printStackTrace();
		} catch (KeyNotMatchException e) {
			e.printStackTrace();
		} catch (IteratorException e) {
			e.printStackTrace();
		} catch (UnpinPageException e) {
			e.printStackTrace();
		} catch (ScanIteratorException e) {
			e.printStackTrace();
		} catch (AddFileEntryException e) {
			e.printStackTrace();
		}

		//sort here
		 if (ordertype == 1) {
		  Collections.sort(query_map, new RowLabelSorter()
						.thenComparing(new ColumnLabelSorter())
						.thenComparing(new TimeStampSorter()));
		 }
		 else if (ordertype==2) {

		  Collections.sort(query_map, new ColumnLabelSorter()
						.thenComparing(new RowLabelSorter())
						.thenComparing(new TimeStampSorter()));
		 }
		 else if (ordertype==3) {

		  Collections.sort(query_map, new RowLabelSorter()
						.thenComparing(new TimeStampSorter()));
		 }
		 else if (ordertype==4) {

		  Collections.sort(query_map, new ColumnLabelSorter()
						.thenComparing(new TimeStampSorter()));
		 }
		 else if (ordertype==5) {

		  Collections.sort(query_map, new TimeStampSorter());

		 }
	}

	public static ArrayList<MAP> query(bigT db, String rowFilter, String columnFilter, String valueFilter, int orderType) throws IOException, GetFileEntryException, PinPageException, ConstructPageException, KeyNotMatchException, IteratorException, UnpinPageException, ScanIteratorException, AddFileEntryException
	{
		HashMap<String, MAP> stream_map = new HashMap<String, MAP>();
		ArrayList<MAP> result = new ArrayList<MAP>();
		MAP map;

		//index types
		ArrayList<MID> mids = new ArrayList<MID>();
		KeyClass r_key;
		KeyClass c_key;
		KeyClass rv_key;
		KeyClass rc_key;

		for(int index_type: db.type_used) {
			String name = db.name;
			switch (index_type) {
				case 1:
					//no index
					boolean done = false;
					MID search_mid = new MID();
					map = new MAP();
					Heapfile hf = null;
					try {
						hf = new Heapfile(name + index_type + "DATA");
					} catch (HFException e) {
						e.printStackTrace();
					} catch (HFBufMgrException e) {
						e.printStackTrace();
					} catch (HFDiskMgrException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}

					Scan sc = null;
					try {
						sc = hf.openScan();
					} catch (InvalidTupleSizeException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}

					while (!done) {
						try {
							map = sc.getNext(search_mid);
						} catch (InvalidTupleSizeException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}

						if (map == null) {
							done = true;
							break;
						}
						if (filterComp(1, rowFilter, map) == true && filterComp(2, columnFilter, map) == true && filterComp(3, valueFilter, map) == true) {
							if (orderType == 0)
							{
								result.add(map);
							}
							else
							{
								stream_map.put(search_mid.pageNo + ":" + search_mid.slotNo + ":" + index_type, map);
							}
						}
					}
					sc.closescan();
					break;
				case 2:
					//index row labels
					if (rowFilter.charAt(0) == '[') //splits the label if it is a range
					{
						String[] range = rowFilter.split(",");
						r_key = new StringKey(range[0].substring(1, range[0].length()));
						KeyClass r_key2 = new StringKey(range[1].substring(0, range[1].length() - 1));
						mids = db.scanForKey(name + "R_INDEX", r_key, r_key2, index_type);
					} else if (rowFilter.charAt(0) == '*') //scan entire index
					{
						mids = db.scanForKey(name + "R_INDEX", null, null, index_type);
					} else //does not split if it is not a range
					{
						r_key = new StringKey(rowFilter);
						mids = db.scanForKey(name + "R_INDEX", r_key, r_key, index_type);
					}

					for (int i = 0; i < mids.size(); i++) //loop of maps that satisfy the index
					{
						map = db.getMap(mids.get(i),index_type);
						if (map == null)
						{
							continue;
						}
						else if (filterComp(1, rowFilter, map) == true && filterComp(2, columnFilter, map) == true && filterComp(3, valueFilter, map) == true) {
							stream_map.put(mids.get(i).pageNo + ":" + mids.get(i).slotNo + ":" + index_type, map);
						}
					}
					break;
				case 3:
					//index column labels
					if (columnFilter.charAt(0) == '[') //splits the label if it is a range
					{
						String[] range = columnFilter.split(",");
						c_key = new StringKey(range[0].substring(1, range[0].length()));
						KeyClass c_key2 = new StringKey(range[1].substring(0, range[1].length() - 1));
						mids = db.scanForKey(name + "C_INDEX", c_key, c_key2, index_type);
					} else if (columnFilter.charAt(0) == '*') //scan entire index
					{
						mids = db.scanForKey(name + "C_INDEX", null, null, index_type);
					} else //does not split if it is not a range
					{
						c_key = new StringKey(columnFilter);
						mids = db.scanForKey(name + "C_INDEX", c_key, c_key, index_type);
					}

					for (int i = 0; i < mids.size(); i++) //loop of maps that satisfy the index
					{
						map = db.getMap(mids.get(i),index_type);

						if (map == null)
						{
							continue;
						}
						else if (filterComp(1, rowFilter, map) == true && filterComp(2, columnFilter, map) == true && filterComp(3, valueFilter, map) == true) {
							stream_map.put(mids.get(i).pageNo + ":" + mids.get(i).slotNo + ":" + index_type, map);
						}
					}
					break;
				case 4:
					//index col and label and timestamp
					String colf1;
					String colf2;
					String rowf1;
					String rowf2;

					if (columnFilter.charAt(0) == '[') //splits the label if it is a range
					{
						//return a range
						String[] range = columnFilter.split(",");
						colf1 = range[0].substring(1, range[0].length());
						colf2 = range[1].substring(0, range[1].length() - 1);
					} else //does not split if it is not a range
					{
						colf1 = columnFilter;
						colf2 = columnFilter;
					}

					if (rowFilter.charAt(0) == '[') //splits the label if it is a range
					{
						String[] range = rowFilter.split(",");
						rowf1 = range[0].substring(1, range[0].length());
						rowf2 = range[1].substring(0, range[1].length() - 1);
					} else //does not split if it is not a range
					{
						rowf1 = rowFilter;
						rowf2 = rowFilter;
					}
					rc_key = new StringKey(rowf1 + colf1);
					KeyClass rc_key2 = new StringKey(rowf2 + colf2);
					if (rowFilter.charAt(0) == '*' || columnFilter.charAt(0) == '*') {
						mids = db.scanForKey(name + "RC_INDEX", null, null, index_type);
					} else {
						mids = db.scanForKey(name + "RC_INDEX", rc_key, rc_key2, index_type);
					}

					for (int i = 0; i < mids.size(); i++) //does not split if it is not a range
					{
						map = db.getMap(mids.get(i),index_type);
						if (map == null)
						{
							continue;
						}
						else if (filterComp(1, rowFilter, map) == true && filterComp(2, columnFilter, map) == true && filterComp(3, valueFilter, map) == true) {
							stream_map.put(mids.get(i).pageNo + ":" + mids.get(i).slotNo + ":" + index_type, map);
						}
					}
					break;
				case 5:
					//index row and value and timestamp
					String rf1;
					String rf2;
					String valf1;
					String valf2;
					if (rowFilter.charAt(0) == '[') //splits the label if it is a range
					{
						//return a range
						String[] range = rowFilter.split(",");
						rf1 = range[0].substring(1, range[0].length());
						rf2 = range[1].substring(0, range[1].length() - 1);
					} else //does not split if it is not a range
					{
						rf1 = rowFilter;
						rf2 = rowFilter;
					}

					if (valueFilter.charAt(0) == '[') //splits the label if it is a range
					{
						String[] range = valueFilter.split(",");
						valf1 = range[0].substring(1, range[0].length());
						valf2 = range[1].substring(0, range[1].length() - 1);
					} else //does not split if it is not a range
					{
						valf1 = valueFilter;
						valf2 = valueFilter;
					}
					rv_key = new StringKey(rf1 + valf1);
					KeyClass rv_key2 = new StringKey(rf2 + valf2);
					if (rowFilter.charAt(0) == '*' || valueFilter.charAt(0) == '*')//scan entire index
					{
						mids = db.scanForKey(name + "RV_INDEX", null, null, index_type);
					} else {
						mids = db.scanForKey(name + "RV_INDEX", rv_key, rv_key2, index_type);
					}

					for (int i = 0; i < mids.size(); i++) //does not split if it is not a range
					{
						map = db.getMap(mids.get(i), index_type);
						if (map == null)
						{
							continue;
						}
						else if (filterComp(1, rowFilter, map) == true && filterComp(2, columnFilter, map) == true && filterComp(3, valueFilter, map) == true) {
							stream_map.put(mids.get(i).pageNo + ":" + mids.get(i).slotNo + ":" + index_type, map);
						}
					}
					break;
			}
		}

		if (orderType != 0)
		{
			for (String result_mid : stream_map.keySet())
			{
				result.add(stream_map.get(result_mid));
			}
		}

		return result;
	}

	public static boolean filterComp(int labelType, String filter, MAP bmap) throws IOException
	{
		boolean satisfyQ = false;
		String mapLabel = "";
		MAP map = bmap;

		//get labels
		if(labelType == 1) //row
		{
			mapLabel = bmap.getRowLabel();
		}
		else if(labelType == 2) //column
		{
			mapLabel = bmap.getColumnLabel();
		}
		else if(labelType == 3) //value
		{
			mapLabel = bmap.getValue();
		}

		//data returned
		if(filter.charAt(0) == '*')
		{
			//return all
			satisfyQ = true;
		}
		else if(filter.charAt(0) == '[')
		{
			//return a range
			filter = filter.substring(1, filter.length() - 1);
			String[] range = filter.split(",");

			if(mapLabel.compareTo(range[0]) >= 0 && mapLabel.compareTo(range[1]) <= 0)
			{
				satisfyQ = true;
			}
			else
			{
				satisfyQ = false;
			}
		}
		else
		{
			//return one
			if(mapLabel.equals(filter))
			{
				satisfyQ = true;
			}
			else
			{
				satisfyQ = false;
			}
		}

		return satisfyQ;
	}

    public void closestream()
    {
    	reset();
    }

    /** Reset everything and unpin all pages. */
    private void reset()
    { 
    	db.closeBigt();
  	}

    // //Retrieve the next map in the stream.
    // //Note that the above methods use a map id class, MID, that needs to be declared in a global.MID, similar to global.RID.
    public void getNext()
    {
    	for (int i = 0; i < query_map.size(); i ++)
		{
			try {
				query_map.get(i).print();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println();
		}
		if (query_map.size() == 0) {
			System.out.println("No maps found");
		}
		else {
			System.out.println("Number of maps found: " + query_map.size());
		}
	}
    
  //retrieves the query map
    public ArrayList<MAP> getArray()
    {
		return query_map;
	}
}

    
    