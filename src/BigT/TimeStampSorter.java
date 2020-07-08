package BigT;

import java.io.IOException;
import java.util.Comparator;

public class TimeStampSorter implements Comparator<MAP>
	{
	    public int compare(MAP o1, MAP o2)
	    {
			try {
				return o1.getTimeStamp() - o2.getTimeStamp();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return 0;
		}
	}
