package carbyne.utils;

public class Pair<K, V> {

  private K first;
  private V second;

  public static <K, V> Pair<K, V> createPair(K element0, V element1) {
    return new Pair<K, V>(element0, element1);
  }

  public Pair(K element0, V element1) {
    this.first = element0;
    this.second = element1;
  }

  public K first() {
    return first;
  }

  public V second() {
    return second;
  }

  public void setFirst(K value) {
    first = value;
  }

  public void setSecond(V value) {
    second = value;
  }

  @Override
  public String toString() {
    return "<" + first + ", " + second + ">";
  }
}
