package global;

/** 
 * Enumeration class for MapOrder
 * 
 */

public class MapOrder {

  public static final int Ascending  = 0;
  public static final int Descending = 1;
  public static final int Random     = 2;

  public int mapOrder;

  /** 
   * MapOrder Constructor
   * <br>
   * A Map ordering can be defined as 
   * <ul>
   * <li>   MapOrder MapOrder = new MapOrder(MapOrder.Random);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (MapOrder.MapOrder == MapOrder.Random) ....
   * </ul>
   *
   * @param _mapOrder The possible ordering of the Maps 
   */

  public MapOrder (int _mapOrder) {
    mapOrder = _mapOrder;
  }

  public String toString() {
    
    switch (mapOrder) {
    case Ascending:
      return "Ascending";
    case Descending:
      return "Descending";
    case Random:
      return "Random";
    }
    return ("Unexpected MapOrder " + mapOrder);
  }

}
