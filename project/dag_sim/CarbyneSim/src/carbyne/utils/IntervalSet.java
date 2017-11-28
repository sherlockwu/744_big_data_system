package carbyne.utils;

import java.util.Map;
import java.util.TreeMap;

/// Invariant: the intervals are disjoint; key is the begin
public class IntervalSet {

  public Map<Integer, Interval> intervals;

  public IntervalSet() {
    intervals = new TreeMap<Integer, Interval>();
  }

  // O(#intervals) operation
  public void Remove(int i) {
    if (intervals.size() == 0) {
      return;
    }

    Interval toRemove = null;
    for (Interval intval : intervals.values()) {
      if (intval.begin <= i && intval.end >= i) {
        toRemove = intval;
        break;
      }
    }

    if (toRemove != null) {
      intervals.remove(toRemove.begin);

      if (i - 1 >= toRemove.begin) {
        intervals.put(toRemove.begin, new Interval(toRemove.begin, i - 1));
      }

      if (i + 1 <= toRemove.end) {
        intervals.put(i + 1, new Interval(i + 1, toRemove.end));
      }
    }
  }

  // O(#intervals) operation
  public boolean Contains(Interval interval) {
    if (intervals.size() == 0) {
      return false;
    }
    for (Interval intval : intervals.values()) {
      if (intval.begin <= interval.begin && intval.end >= interval.end) {
        return true;
      }
    }
    return false;
  }

  // O(#intervals) operation
  public boolean Overlaps(Interval interval) {
    if (intervals.size() == 0) {
      return false;
    }
    for (Interval intval : intervals.values()) {
      if (intval.begin <= interval.end && intval.end >= interval.begin) {
        return true;
      }
    }
    return false;
  }

}
