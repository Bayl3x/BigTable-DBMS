
package diskmgr;

import java.io.*;
import bufmgr.*;
import global.*;

public class pcounter
{
	public static int rcount;
	public static int wcount;
	
	public static void initialize()
	{
		rcount = 0;
		wcount = 0;
	}
	
	public static void readInc()
	{
		rcount++;
	}
	
	public static void writeInc()
	{
		wcount++;
	}
	
	public static int getreadCount()
	{
		return rcount;
	}
	
	public static int getwriteCount()
	{
		return wcount;
	}
}