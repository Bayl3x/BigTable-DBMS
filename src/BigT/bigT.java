package BigT;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.lang.*;

import btree.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import java.util.HashMap;
import java.util.Map;

import heap.*;
import iterator.MapUtils;
import static global.GlobalConst.*;

public class bigT
{
    public Set<Integer> type_used;
    int totalMapCount;
    Set<String> total_distinct_row;
    Set<String> total_distinct_column;
    String name;
    boolean sorting;
    //Initialize the big table. type is an integer be- tween 1 and 5 and the different types will correspond to different
    //clustering and indexing strategies you will use for the bigtable.
    public bigT(String name) {
        this.sorting = false;
        this.name = name;
        this.type_used = new HashSet<Integer>();
        this.totalMapCount = 0;
        this.total_distinct_row = new HashSet<String>();
        this.total_distinct_column = new HashSet<String>();
    }

    //Delete the bigtable from the database.
    public void deleteBigt()
    {
        try {
            SystemDefs.JavabaseDB.DBDestroy();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clearBuffer()
    {
        for (int i = 0; i < INITIAL_NUM_OF_DATA_PAGES; i ++)
        {
            PageId close_pid = new PageId(i);

            try {
                SystemDefs.JavabaseBM.unpinPage(close_pid, true);
            } catch (ReplacerException e) {
            } catch (PageUnpinnedException e) {
            } catch (HashEntryNotFoundException e) {
            } catch (InvalidFrameNumberException e) {
            }
        }

        try {
            SystemDefs.JavabaseBM.flushAllPages();
        } catch (HashOperationException e) {
            e.printStackTrace();
        } catch (PageUnpinnedException e) {
            e.printStackTrace();
        } catch (PagePinnedException e) {
            e.printStackTrace();
        } catch (PageNotFoundException e) {
            e.printStackTrace();
        } catch (BufMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeBigt()
    {
        clearBuffer();

        try {
            SystemDefs.JavabaseDB.closeDB();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Return number of maps in the bigtable.
    public void getCount()
    {
        System.out.println("DB: " + name);
        System.out.println("Map count: " + totalMapCount);
        System.out.println("Distinct row count: " + total_distinct_row.size());
        System.out.println("Distinct column count: " + total_distinct_column.size());
    }

    public void deleteRecord(MID mid, int type) throws ConstructPageException, GetFileEntryException, AddFileEntryException, IOException {
        MAP deleteMap = new MAP();

        deleteMap = getMap(mid, type);
        KeyClass r_key;
        KeyClass c_key;
        KeyClass rv_key;
        KeyClass rc_key;
        KeyClass t_key;

        switch (type)
        {
            case 1:
                rc_key = new StringKey(deleteMap.getRowLabel() + deleteMap.getColumnLabel());
                deleteIndex(rc_key ,mid, name + type + "INSERT_INDEX");
                break;
            case 2:
                r_key = new StringKey(deleteMap.getRowLabel());
                rc_key = new StringKey(deleteMap.getRowLabel() + deleteMap.getColumnLabel());
                deleteIndex(r_key ,mid, name + "R_INDEX");
                deleteIndex(rc_key ,mid, name + type + "INSERT_INDEX");
                break;
            case 3:
                c_key = new StringKey(deleteMap.getColumnLabel());
                rc_key = new StringKey(deleteMap.getRowLabel() + deleteMap.getColumnLabel());
                deleteIndex(c_key ,mid, name + "C_INDEX");
                deleteIndex(rc_key ,mid, name + type + "INSERT_INDEX");
                break;
            case 4:
                t_key = new IntegerKey(deleteMap.getTimeStamp());
                rc_key = new StringKey(deleteMap.getRowLabel() + deleteMap.getColumnLabel());
                deleteIndex(t_key, mid, name + "T_INDEX");
                deleteIndex(rc_key, mid, name + "RC_INDEX");
                deleteIndex(rc_key ,mid, name + type + "INSERT_INDEX");
                break;
            case 5:
                t_key = new IntegerKey(deleteMap.getTimeStamp());
                rv_key = new StringKey(deleteMap.getRowLabel() + deleteMap.getValue());
                rc_key = new StringKey(deleteMap.getRowLabel() + deleteMap.getColumnLabel());
                deleteIndex(t_key, mid, name + "T_INDEX");
                deleteIndex(rv_key, mid, name + "RV_INDEX");
                deleteIndex(rc_key ,mid, name + type + "INSERT_INDEX");
                break;
        }

        Heapfile hf = null;
        //data file
        try {
            hf = new Heapfile(name + type + "DATA");
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            hf.deleteRecord(mid);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (!sorting)
        {
            totalMapCount --;
        }
    }

    public MAP getMap(MID mid, int type)
    {
        MAP map = new MAP();

        Heapfile hf = null;
        //data file
        try {
            hf = new Heapfile(name + type + "DATA");
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            map = hf.getRecord(mid);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    //Insert map into the big table, return its Mid.
    //The insertMap() method ensures that there are at most three maps with the same row and column la-bels, but different timestamps,
    //in the bigtable.When a fourth is inserted, the one with the oldest label is dropped from the big table.
    public MID insertMap(String name, int type, byte[] mapPtr) throws IOException, ConstructPageException, GetFileEntryException, AddFileEntryException {
        type_used.add(type);

        if (!sorting) {
            //find 3 map with same R,C
            Map<MID, MAP> search_result = search(mapPtr);

            //sort by time stamp and delete oldest map
            if (search_result.size() >= 3) {
                List<MID> list = new ArrayList<>(search_result.keySet());
                Collections.sort(list, (w1, w2) -> {
                    int re = 0;
                    try {
                        re = search_result.get(w2).getTimeStamp() - search_result.get(w1).getTimeStamp();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return re;
                });

                deleteRecord(list.get(search_result.size() - 1), list.get(search_result.size() - 1).type);
            }
        }

        MID mid = new MID();

        Heapfile hf = null;
        //data file
        try {
            hf = new Heapfile(name + type + "DATA");
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            mid = hf.insertRecord(mapPtr);
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (SpaceNotAvailableException e) {
            e.printStackTrace();
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        }

        MAP dataMap = new MAP(mapPtr);

        if (!sorting) {
            total_distinct_row.add(dataMap.getRowLabel());
            total_distinct_column.add(dataMap.getColumnLabel());
            totalMapCount++;
        }

        return mid;
    }

    //used to find the 3 instances of same row and column
    Map<MID,MAP> search(byte[] mapPtr) throws IOException {
        Map<MID, MAP> result = new HashMap<>();
        MAP mmp = new MAP(mapPtr);
        MAP search_map = new MAP();
        KeyClass search_key = null;
        ArrayList<MID> mids = null;
        boolean done = false;

        mids = new ArrayList<MID>();

        try {
            search_key = new StringKey(mmp.getRowLabel() + mmp.getColumnLabel());
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(int type: type_used) {
            try {
                mids = scanForKey(name + type + "INSERT_INDEX", search_key, search_key, type);
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

            for (int i = 0; i < mids.size(); i++) {
                search_map = getMap(mids.get(i), type);
                if (search_map != null) {
                    if (MapUtils.compareRowCol(search_map, mmp))
                    {
                        mids.get(i).type = type;
                        result.put(mids.get(i), search_map);
                    }
                }
            }
        }

        return result;
    }

    public void fileSort (int fileType) throws IteratorException, ScanIteratorException, ConstructPageException, UnpinPageException, PinPageException, AddFileEntryException, GetFileEntryException, KeyNotMatchException, IOException, NodeNotMatchException, LeafDeleteException, LeafInsertRecException, IndexSearchException, InsertException, ConvertException, DeleteRecException, KeyTooLongException, IndexInsertRecException {
        sorting = true;
        ArrayList<MID> mids = new ArrayList<MID>();
        MAP sortMap = new MAP();
        MID insertMid = new MID();
        KeyClass r_key;
        KeyClass c_key;
        KeyClass rv_key;
        KeyClass rc_key;
        KeyClass t_key;

        switch (fileType)
        {
            case 1:
                mids = scanForKey(name + fileType + "INSERT_INDEX", null, null, fileType);
                for(MID sortMid: mids)
                {
                    sortMap = getMap(sortMid, fileType);
                    deleteRecord(sortMid, fileType);
                    insertMid = insertMap(name, fileType, sortMap.getMapByteArray());
                    rc_key = new StringKey(sortMap.getRowLabel() + sortMap.getColumnLabel());
                    if (insertMid != null) {
                        insertIndex(rc_key, insertMid, name + fileType + "INSERT_INDEX");
                    }
                }
                break;
            case 2:
                mids = scanForKey(name + "R_INDEX", null, null, fileType);
                for(MID sortMid: mids)
                {
                    sortMap = getMap(sortMid, fileType);
                    deleteRecord(sortMid, fileType);
                    insertMid = insertMap(name, fileType, sortMap.getMapByteArray());
                    r_key = new StringKey(sortMap.getRowLabel());
                    rc_key = new StringKey(sortMap.getRowLabel() + sortMap.getColumnLabel());
                    if (insertMid != null) {
                        insertIndex(r_key, insertMid, name + "R_INDEX");
                        insertIndex(rc_key, insertMid, name + fileType + "INSERT_INDEX");
                    }
                }
                break;
            case 3:
                mids = scanForKey(name + "C_INDEX", null, null, fileType);
                for(MID sortMid: mids)
                {
                    sortMap = getMap(sortMid, fileType);
                    deleteRecord(sortMid, fileType);
                    insertMid = insertMap(name, fileType, sortMap.getMapByteArray());
                    c_key = new StringKey(sortMap.getColumnLabel());
                    rc_key = new StringKey(sortMap.getRowLabel() + sortMap.getColumnLabel());
                    if (insertMid != null) {
                        insertIndex(c_key, insertMid, name + "C_INDEX");
                        insertIndex(rc_key, insertMid, name + fileType + "INSERT_INDEX");
                    }
                }
                break;
            case 4:
                mids = scanForKey(name + "RC_INDEX", null, null, fileType);
                for(MID sortMid: mids)
                {
                    sortMap = getMap(sortMid, fileType);
                    deleteRecord(sortMid, fileType);
                    insertMid = insertMap(name, fileType, sortMap.getMapByteArray());
                    rc_key = new StringKey(sortMap.getRowLabel() + sortMap.getColumnLabel());
                    t_key = new IntegerKey(sortMap.getTimeStamp());
                    if (insertMid != null) {
                        insertIndex(t_key, insertMid, name + "T_INDEX");
                        insertIndex(rc_key, insertMid, name + "RC_INDEX");
                        insertIndex(rc_key, insertMid, name + fileType + "INSERT_INDEX");
                    }
                }
                break;
            case 5:
                mids = scanForKey(name + "RV_INDEX", null, null, fileType);
                for(MID sortMid: mids)
                {
                    sortMap = getMap(sortMid, fileType);
                    deleteRecord(sortMid, fileType);
                    insertMid = insertMap(name, fileType, sortMap.getMapByteArray());
                    rv_key = new StringKey(sortMap.getRowLabel() + sortMap.getValue());
                    t_key = new IntegerKey(sortMap.getTimeStamp());
                    rc_key = new StringKey(sortMap.getRowLabel() + sortMap.getColumnLabel());
                    if (insertMid != null) {
                        insertIndex(rv_key, insertMid, name + "RV_INDEX");
                        insertIndex(t_key, insertMid, name + "T_INDEX");
                        insertIndex(rc_key, insertMid, name + fileType + "INSERT_INDEX");
                    }
                }
                break;
        }
        sorting = false;
    }

    public void insertIndex(KeyClass key, MID mid, String index_name) throws GetFileEntryException, ConstructPageException, AddFileEntryException, IOException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException
    {
        int keyType = AttrType.attrNull;
        int keySize = 0;

        if(index_name.contains("R_INDEX")) {
            keyType = AttrType.attrString;
            //NOTE: May need to change keySize values
            keySize = ROW_SIZE;
        }
        else if(index_name.contains("RC_INDEX")) {
            keyType = AttrType.attrString;
            keySize = ROW_SIZE + COLUMN_SIZE;
        }
        else if(index_name.contains("INSERT_INDEX")) {
            keyType = AttrType.attrString;
            keySize = ROW_SIZE + COLUMN_SIZE;
        }
        else if(index_name.contains("RV_INDEX")) {
            keyType = AttrType.attrString;
            keySize = ROW_SIZE + VALUE_SIZE;
        }
        else if(index_name.contains("C_INDEX")) {
            keyType = AttrType.attrString;
            keySize = COLUMN_SIZE;
        }
        else if(index_name.contains("T_INDEX")) {
            keyType = AttrType.attrInteger;
            keySize = TIMESTAMP_SIZE;
        }

        BTreeFile bt = new BTreeFile(index_name, keyType, keySize, 0);
        bt.insert(key, mid);

        try {
            bt.close();
        } catch (PageUnpinnedException e) {
            e.printStackTrace();
        } catch (InvalidFrameNumberException e) {
            e.printStackTrace();
        } catch (HashEntryNotFoundException e) {
            e.printStackTrace();
        } catch (ReplacerException e) {
            e.printStackTrace();
        }
    }

    public void deleteIndex(KeyClass key, MID mid, String index_name) throws IOException, AddFileEntryException, GetFileEntryException, ConstructPageException {
        int keyType = AttrType.attrNull;
        int keySize = 0;

        if(index_name.contains("R_INDEX")) {
            keyType = AttrType.attrString;
            //NOTE: May need to change keySize values
            keySize = ROW_SIZE;
        }
        else if(index_name.contains("RC_INDEX")) {
            keyType = AttrType.attrString;
            keySize = ROW_SIZE + COLUMN_SIZE;
        }
        else if(index_name.contains("INSERT_INDEX")) {
            keyType = AttrType.attrString;
            keySize = ROW_SIZE + COLUMN_SIZE;
        }
        else if(index_name.contains("RV_INDEX")) {
            keyType = AttrType.attrString;
            keySize = ROW_SIZE + VALUE_SIZE;
        }
        else if(index_name.contains("C_INDEX")) {
            keyType = AttrType.attrString;
            keySize = COLUMN_SIZE;
        }
        else if(index_name.contains("T_INDEX")) {
            keyType = AttrType.attrInteger;
            keySize = TIMESTAMP_SIZE;
        }

        BTreeFile bt = new BTreeFile(index_name, keyType, keySize, 0);

        try {
            bt.Delete(key, mid);
        }
        catch(Exception e) {
            e.printStackTrace();
        }

        try {
            bt.close();
        } catch (PageUnpinnedException e) {
            e.printStackTrace();
        } catch (InvalidFrameNumberException e) {
            e.printStackTrace();
        } catch (HashEntryNotFoundException e) {
            e.printStackTrace();
        } catch (ReplacerException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<MID> scanForKey(String index_name, KeyClass lo_key, KeyClass hi_key, int type) throws GetFileEntryException, PinPageException, ConstructPageException, KeyNotMatchException, IteratorException, UnpinPageException, IOException, ScanIteratorException, AddFileEntryException {

        int keyType = AttrType.attrNull;
        int keySize = 0;

        if(index_name.contains("R_INDEX")) {
            keyType = AttrType.attrString;
            //NOTE: May need to change keySize values
            keySize = ROW_SIZE;
        }
        else if(index_name.contains("RC_INDEX")) {
            keyType = AttrType.attrString;
            keySize = ROW_SIZE + COLUMN_SIZE;
        }
        else if(index_name.contains("INSERT_INDEX")) {
            keyType = AttrType.attrString;
            keySize = ROW_SIZE + COLUMN_SIZE;
        }
        else if(index_name.contains("RV_INDEX")) {
            keyType = AttrType.attrString;
            keySize = ROW_SIZE + VALUE_SIZE;
        }
        else if(index_name.contains("C_INDEX")) {
            keyType = AttrType.attrString;
            keySize = COLUMN_SIZE;
        }
        else if(index_name.contains("T_INDEX")) {
            keyType = AttrType.attrInteger;
            keySize = TIMESTAMP_SIZE;
        }

        HashMap<String, MID> mids = new HashMap<String, MID>();

        BTreeFile bt = new BTreeFile(index_name, keyType, keySize, 0);

        BTFileScan scanner = bt.new_scan(lo_key, hi_key);
        boolean done = false;

        while(!done) {
            KeyDataEntry kd = scanner.get_next();

            if (kd == null) {
                done = true;
                break;
            }

            if(kd.data instanceof LeafData) {
                mids.put(((LeafData) kd.data).getData().pageNo + ":" + ((LeafData) kd.data).getData().slotNo + ":" + type, ((LeafData) kd.data).getData());
            }
        }

        ArrayList<MID> result = new ArrayList<MID>();

        for(String mid: mids.keySet())
        {
            result.add(mids.get(mid));
        }

        try {
            bt.close();
        } catch (PageUnpinnedException e) {
            e.printStackTrace();
        } catch (InvalidFrameNumberException e) {
            e.printStackTrace();
        } catch (HashEntryNotFoundException e) {
            e.printStackTrace();
        } catch (ReplacerException e) {
            e.printStackTrace();
        }

        return result;
    }

    //Initialize a stream of maps where row label matching rowFilter, column label matching columnFilter, and value
    //label matching valueFilter.If any of the filter are null strings, then that filter is not considered
    //(e.g ., if rowFilter is null, then all row labels are OK).
    //If orderType is
    //1: then results are first ordered in row label, then column label, then time stamp.
    //2, then results are first ordered in column label, then row label, then time stamp.
    //3, then results are first ordered in row label, then time stamp.
    //4, then results are first ordered in column label, then time stamp
    //5, then results are ordered in time stamp
    public Stream openStream(bigT db, String name, int ordertype, String rowFilter, String columnFilter, String valueFilter, int numbuf)
    {
    	Stream newstream = new Stream(db, name, ordertype, rowFilter, columnFilter, valueFilter, numbuf);
        return newstream;
    }
}
