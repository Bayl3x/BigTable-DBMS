package BigT;

import java.io.IOException;
import java.util.Comparator;

public class ColumnLabelSorter implements Comparator<MAP>
	{
	    public int compare(MAP o1, MAP o2)
	    {
			try {
				return o1.getColumnLabel().compareTo(o2.getColumnLabel());
			} catch (IOException e) {
				e.printStackTrace();
			}
			return 0;
		}
	}