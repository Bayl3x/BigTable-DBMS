package BigT;// package tuples;

import java.io.*;
import java.lang.*;
import java.util.Arrays;

import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import static global.GlobalConst.*;

// You will need to create a class BigT.Map, similar to heap.Tuple but having a fixed structure
// (and thus a fixed header) as described above. Thus, the constructor and get/set methods associated with the
// BigT.Map should be adapted as appropriate:
// <String, String, Integer, Integer>
public class MAP {
	private byte [] data;
	private byte [] row_label = new byte[ROW_SIZE];		//string length =20-2 = 18
	private byte [] column_label = new byte[COLUMN_SIZE]; 	// string length = 18
	private byte [] timestamp = new byte[TIMESTAMP_SIZE];  		// integer can be a lager number
	private byte [] value = new byte[VALUE_SIZE];     		// integer string length = 8
	private int map_length = MAP_SIZE;
	private short fldCnt = 4;
//	private int tuple_offset = 0;
//	private short [] fldOffset; 
	
	// Class constructor create a new map with the appropriate size. 
	public MAP(){
		data = new byte[map_length];
		// map_length = max_size;
	}

//    * Class constructor
//    * Creat a new tuple with length = size,tuple offset = 0.
//    */
 
  public  MAP(int size)
  {
       // Creat a new map
       data = new byte[size];
//       tuple_offset = 0;
//       tuple_length = size;     
  }
	// Construct a map from a byte array.
	public MAP(byte[] amap){
		data = amap;
	}

	// Construct a map from another map through copy.
	public MAP(MAP fromMap){
		data = fromMap.getMapByteArray();
		// map_length = fromMap.size();
		// fldCnt = fromMap.noOfFlds();
		// fldOffset = fromMap.copyFldOffset();  
	}

	// System.arraycopy(src, srcPos, dest, destPos, length);
	// src: resource array, srcPos: begin position, dest: target array, destPos: target position
	// length
	// Returns the row label
	public String getRowLabel() throws IOException {
		String row_label;
		InputStream in;
		DataInputStream instr;
		// int value;
		byte tmp[] = Arrays.copyOfRange(data,0,30);

		// copy the value from data array out to a tmp byte array
		// System.arraycopy (data, 0, tmp, 0, 20);

		/* creates a new data input stream to read data from the
		* specified input stream
		*/
		in = new ByteArrayInputStream(tmp);
		instr = new DataInputStream(in);
		row_label = instr.readUTF();
		return row_label;
	}

	// Returns the column label.
	public String getColumnLabel() throws IOException {
		String column_label;
		InputStream in;
		DataInputStream instr;
		// int value;
		byte tmp[] = Arrays.copyOfRange(data,30,50);

		// copy the value from data array out to a tmp byte array
		// System.arraycopy (data, 20, tmp, 0, 20);

		/* creates a new data input stream to read data from the
		* specified input stream
		*/
		in = new ByteArrayInputStream(tmp);
		instr = new DataInputStream(in);
		column_label = instr.readUTF();
		return column_label;
	}

	// Returns the timestamp.
	public int getTimeStamp() throws IOException {
		int time;
		InputStream in;
		DataInputStream instr;
		// int value;
		byte tmp[] = Arrays.copyOfRange(data,60,70);

		/* creates a new data input stream to read data from the
		* specified input stream
		*/
		in = new ByteArrayInputStream(tmp);
		instr = new DataInputStream(in);
		time = instr.readInt();
		return time;
	}

	// Returns the value.
	public String getValue() throws IOException {
		String value;
		InputStream in;
		DataInputStream instr;
		// int value;
		byte tmp[] = Arrays.copyOfRange(data,50,60);

		/* creates a new data input stream to read data from the
		* specified input stream
		*/
		in = new ByteArrayInputStream(tmp);
		instr = new DataInputStream(in);
		value = instr.readUTF();
		return value;
	}

	// Set the row label.
	public void setRowLabel(String val){
		/* creates a new data output stream to write data to
		* underlying output stream
		*/

		OutputStream out = new ByteArrayOutputStream();
		DataOutputStream outstr = new DataOutputStream (out);

		// write the value to the output stream

		try {
			outstr.writeUTF(val);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// creates a byte array with this output stream size and the 
		// valid contents of the buffer have been copied into it
		byte []B = ((ByteArrayOutputStream) out).toByteArray();

		int sz =outstr.size();  
		// copies the contents of this byte array into data[]
		System.arraycopy (B, 0, data, 0, sz);
	}

	// Set the column label.
	public void setColumnLabel(String val){
		/* creates a new data output stream to write data to
		* underlying output stream
		*/

		OutputStream out = new ByteArrayOutputStream();
		DataOutputStream outstr = new DataOutputStream (out);

		// write the value to the output stream

		try {
			outstr.writeUTF(val);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// creates a byte array with this output stream size and the 
		// valid contents of the buffer have been copied into it
		byte []B = ((ByteArrayOutputStream) out).toByteArray();

		int sz =outstr.size();  
		// copies the contents of this byte array into data[]
		System.arraycopy (B, 0, data, 30, sz);
	}

	// Set the timestamp.
	public void setTimeStamp(int val){
		/* creates a new data output stream to write data to
		* underlying output stream
		*/

		OutputStream out = new ByteArrayOutputStream();
		DataOutputStream outstr = new DataOutputStream (out);

		// write the value to the output stream

		try {
			outstr.writeInt(val);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// creates a byte array with this output stream size and the 
		// valid contents of the buffer have been copied into it
		byte []B = ((ByteArrayOutputStream) out).toByteArray();

		int sz =outstr.size();  
		// copies the contents of this byte array into data[]
		System.arraycopy (B, 0, data, 60, sz);
	}

	// Set the value.
	public void setValue(String val){
		/* creates a new data output stream to write data to
		* underlying output stream
		*/

		OutputStream out = new ByteArrayOutputStream();
		DataOutputStream outstr = new DataOutputStream (out);

		// write the value to the output stream

		try {
			outstr.writeUTF(val);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// creates a byte array with this output stream size and the 
		// valid contents of the buffer have been copied into it
		byte []B = ((ByteArrayOutputStream) out).toByteArray();
		int sz =outstr.size();  
		// copies the contents of this byte array into data[]
		System.arraycopy (B, 0, data, 50, sz);

	}

	// Copy the map to byte array out.
	public byte[] getMapByteArray(){
		byte [] mapcopy = new byte [map_length];
		System.arraycopy(data, 0, mapcopy, 0, map_length);
		return mapcopy;
	}

	// Copy the map to byte array out.
	public byte[] getMapData(){
		return data;
	}

	// Print out the map.
	public void print() throws IOException {
		System.out.print("[");
		for (int i=0; i< fldCnt-1; i++){
			if(i==0){
				String val = getRowLabel();
       			System.out.print(val);
			}
			else if (i==1) {
				String val = getColumnLabel();
       			System.out.print(val);
			}
			else if (i==2) {
				int val = getTimeStamp();
       			System.out.print(val);
			}
			System.out.print(", ");
		}
		String val = getValue();
		System.out.print(val);
		System.out.print("]");
	}

	// Get the length of the map
	public int size(){
		return map_length;
	}

	// Copy the given map
	public void mapCopy(MAP fromMap){
		byte [] temparray = fromMap.getMapByteArray();
		System.arraycopy(temparray, 0, data, 0, map_length);
	} 

	// This is used when you don’t want to use the constructor
	public void mapInit(byte[] amap){
		data = amap;
	}

	// Set a map with the given byte array and offset.
	public void mapSet(byte[] frommap, int offset){
		System.arraycopy(frommap, offset, data, 0, map_length);
	}
	  /**
	   * Set this field to integer value
	   *
	   * @param	fldNo	the field number
	   * @param	val	the integer value
	   * @exception   IOException I/O errors
	   * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
	   */

	  public MAP setIntFld(int fldNo, int val) 
	  	throws IOException, FieldNumberOutOfBoundException
	  { 
		  if ( fldNo == 3)    
	     {
		Convert.setIntValue (val, 50, data);
		return this;
	     }
	    else 
	     throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND"); 
	  }

	  /**
	   * Set this field to float value
	   *
	   * @param     fldNo   the field number
	   * @param     val     the float value
	   * @exception   IOException I/O errors
	   * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
	   */

	  public MAP setFloFld(int fldNo, float val) 
	  	throws IOException, FieldNumberOutOfBoundException
	  { 
	   if ( (fldNo > 0) && (fldNo <= fldCnt))
	    {
//	     Convert.setFloValue (val, fldOffset[fldNo -1], data);
	     return this;
	    }
	    else  
	     throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND"); 
	     
	  }

	  /**
	   * Set this field to String value
	   *
	   * @param     fldNo   the field number
	   * @param     val     the string value
	   * @exception   IOException I/O errors
	   * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
	   */

	   public MAP setStrFld(int fldNo, String val) 
			throws IOException, FieldNumberOutOfBoundException  
	   {
	     if ( fldNo == 1)        
	      {
	         Convert.setStrValue (val, 0, data);
	         return this;
	      }
	     if( fldNo == 2)
	      {
	         Convert.setStrValue (val, 30, data);
	         return this;
	      }
	     else 
	       throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
	    }

}
