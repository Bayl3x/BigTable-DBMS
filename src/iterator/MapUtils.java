package iterator;


import heap.*;
import global.*;
import java.io.*;
import java.lang.*;
import BigT.*;

/**
 *some useful methods when processing Maps 
 */
public class MapUtils
{
  
  /**
   * This function compares a tuple with another tuple in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the tuple is greater,
   *   -1        if the tuple is smaller,
   *
   *@param    m1        one map.
   *@param    m2        another map.
   *@param    map_fld_no the field number to be compared -- 0 corresponds to row value, 1 to column value, 2 to timestamp, and 3 to value.
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,                              
   */
  public static int CompareMapWithMap(
					  MAP m1, MAP m2, int map_fld_no)
    {
        String m1ValString = "";
        String m2ValString = "";

        int m1ValInt = 0;
        int m2ValInt = 0;

        if(map_fld_no == 0 || map_fld_no == 1 || map_fld_no == 3) {
            switch(map_fld_no) {
                case 0:
                	try {
	                    m1ValString = m1.getRowLabel();
	                    m2ValString = m2.getRowLabel();
                	} 
                	catch(IOException e) {
                		System.out.println("IOException thrown");
                	}
                    break;
                case 1:
                	try {
	                    m1ValString = m1.getColumnLabel();
	                    m2ValString = m2.getColumnLabel();
                	}
                	catch(IOException e) {
                		System.out.println("IOException thrown");
                	}
                    break;
                case 3:
                	try {
	                    m1ValString = m1.getValue();
	                    m2ValString = m2.getValue();
                	}
                	catch(IOException e) {
                		System.out.println("IOException thrown");
                	}
                    break;
            }

            int result = m1ValString.compareTo(m2ValString);

            if(result > 0) {
                return 1;
            }
            else if(result < 0) {
                return -1;
            }
            else {
                return 0;
            }

        }
        else {
        	try {
	            m1ValInt = m1.getTimeStamp();
	            m2ValInt = m2.getTimeStamp();
        	}
        	catch(IOException e) {
        		System.out.println("IOException thrown");
        	}

            if(m1ValInt > m2ValInt) {
                return 1;
            }
            else if(m1ValInt < m2ValInt) {
                return -1;
            }
            else {
                return 0;
            }
        }
    }
    
  /**
   *This function Compares two Maps over all fields 
   * @param m1 the first map
   * @param m2 the secocnd map
   * @return  0        if the two are not equal,
   *          1        if the two are equal,
   */            
  
  public static boolean Equal(MAP m1, MAP m2)
    {
		  try {
		        String m1Row = m1.getRowLabel();
		        String m2Row = m2.getRowLabel();
		
		        String m1Col = m1.getColumnLabel();
		        String m2Col = m2.getColumnLabel();
		
		        int m1Time = m1.getTimeStamp();
		        int m2Time = m2.getTimeStamp();
		
		        String m1Val = m1.getValue();
		        String m2Val = m2.getValue();
		        
		        return (m1Row.equals(m2Row) && m1Col.equals(m2Col) && m1Time == m2Time && m1Val.equals(m2Val));
		  }
		  catch(IOException e) {
			System.out.println("IOException thrown");
			return false;
		  }
   
   }
  
  public static boolean compareRowCol(MAP m1, MAP m2) {
	  try {
	        String m1Row = m1.getRowLabel();
	        String m2Row = m2.getRowLabel();
	
	        String m1Col = m1.getColumnLabel();
	        String m2Col = m2.getColumnLabel();
	        
	        return (m1Row.equals(m2Row) && m1Col.equals(m2Col));
	  }
	  catch(IOException e) {
		System.out.println("IOException thrown");
		return false;
	  }
  }
  
  /**
   *get the string specified by the field number
   *@param map the map 
   *@param fidno the field number
   *@return the content of the field number
   */
  public static String Value(MAP  map, int fldno)
    {
      String temp = "";
        switch(fldno) {
            case 0:
            	try {
            		temp = map.getRowLabel();
            	}
      		  	catch(IOException e) {
      		  		System.out.println("IOException thrown");
      		  	}
                break;
            case 1:
            	try {
            		temp = map.getColumnLabel();
            	}
      		  	catch(IOException e) {
      		  		System.out.println("IOException thrown");
      		  	}
                break;
            case 2:
            	try {
            		temp = Integer.toString(map.getTimeStamp());
            	}
      		  	catch(IOException e) {
      		  		System.out.println("IOException thrown");
      		  	}
                break;
            case 3:
            	try {
            		temp = map.getValue();
            	}
      		  	catch(IOException e) {
      		  		System.out.println("IOException thrown");
      		  	}
                break;
            default:
            	try {
            		temp = map.getValue();
            	}
      		  	catch(IOException e) {
      		  		System.out.println("IOException thrown");
      		  	}
                break;
        }
      return temp;
    }
  
 
  /**
   *set up a map in specified field from a map
   *@param value the map to be set 
   *@param map the given map
   *@param fld_no the field number
   */  
  public static void SetValue(MAP value, MAP map, int fld_no)
    {
        switch(fld_no) {
            case 0:
            	try {
	                String newRow = map.getRowLabel();
	                value.setRowLabel(newRow);
            	}
      		  	catch(IOException e) {
      		  		System.out.println("IOException thrown");
      		  	}
                break;
            case 1:
            	try {
	                String newCol = map.getColumnLabel();
	                value.setColumnLabel(newCol);
            	}
      		  	catch(IOException e) {
      		  		System.out.println("IOException thrown");
      		  	}
                break;
            case 2:
            	try {
	                int newTime = map.getTimeStamp();
	                value.setTimeStamp(newTime);
            	}
      		  	catch(IOException e) {
      		  		System.out.println("IOException thrown");
      		  	}
                break;
            case 3:
            	try {
	                String newVal = map.getValue();
	                value.setValue(newVal);
            	}
      		  	catch(IOException e) {
      		  		System.out.println("IOException thrown");
      		  	}
                break;
        }
    }
}
