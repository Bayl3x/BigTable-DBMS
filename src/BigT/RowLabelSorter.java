package BigT;

import java.io.IOException;
import java.util.Comparator;

	public class RowLabelSorter implements Comparator<MAP>
	{
	    public int compare(MAP o1, MAP o2)
	    {
			try {
				return o1.getRowLabel().compareTo(o2.getRowLabel());
			} catch (IOException e) {
				e.printStackTrace();
			}
			return 0;
		}
	}
