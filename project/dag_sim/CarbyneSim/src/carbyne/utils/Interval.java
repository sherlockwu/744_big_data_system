package carbyne.utils;

import java.util.ArrayList;
import java.util.List;

public class Interval {

  public int begin, end;

  public Interval(int b, int e) {
    begin = b;
    end = e;
  }

  public static Interval clone(Interval intval) {
    Interval clonedIntval = new Interval(intval.begin, intval.end);
    return clonedIntval;
  }

  public int Length() {
    return end - begin + 1;
  }

  public List<Integer> toList() {
    List<Integer> list = new ArrayList<Integer>();
    for (int i = begin; i <= end; i++) {
      list.add(i);
    }
    return list;
  }

  public boolean Intersect(Interval b) {
    if (this.end < b.begin || this.begin > b.end)
      return false;
    return true;
  }

  public boolean Intersect(int[] list_tasks) {
    for (int task : list_tasks) {
      if (task >= this.begin && task <= this.end)
        return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return this.begin + " -- " + this.end;
  }
}
