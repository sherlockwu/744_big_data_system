package carbyne.utils;

public class Triple<K, V, Z> {

  private final K first;
  private final V second;
  private final Z third;

  public static <K, V, Z> Triple<K, V, Z> createTriple(K element0, V element1,
      Z element2) {
    return new Triple<K, V, Z>(element0, element1, element2);
  }

  public Triple(K element0, V element1, Z element2) {
    this.first = element0;
    this.second = element1;
    this.third = element2;
  }

  public K first() {
    return first;
  }

  public V second() {
    return second;
  }

  public Z third() {
    return third;
  }
}
