package carbyne.datastructures;

import carbyne.utils.Interval;

public class Dependency {

  public String parent, child;
  public String type;

  public Interval parent_ids, child_ids;

  public Dependency(String p, String c, String t) {
    this.parent = p;
    this.child = c;
    this.type = t;
    this.parent_ids = null;
    this.child_ids = null;
  }

  public Dependency(String p, String c, String t, Interval pi, Interval ci) {
    this.parent = p;
    this.child = c;
    this.type = t;
    this.parent_ids = new Interval(pi.begin, pi.end);
    this.child_ids = new Interval(ci.begin, ci.end);
  }

  public Dependency clone(Dependency dep) {
    Dependency clonedDep = new Dependency(dep.parent, dep.child, dep.type,
        dep.parent_ids, dep.child_ids);
    return clonedDep;
  }

  public Interval getParents(int task) {
    return Dependency.getDependents(task, child_ids, parent_ids, type);
  }

  public Interval getChildren(int task) {
    return Dependency.getDependents(task, parent_ids, child_ids, type);
  }

  /**
   * encodes structure for various dependencies
   * 
   * task - the task in question local_ids - where the task belongs
   * dependent_ids - where the task points at type - pattern
   */
  private static Interval getDependents(int task, Interval local_ids,
      Interval dep_ids, String type) {
    assert (task <= local_ids.end && task >= local_ids.begin);

    if (type.contains("a2a") || type.contains("ata")) {
      return new Interval(dep_ids.begin, dep_ids.end);
    } else if (type.contains("o2o") || type.contains("o2m")
        || type.contains("m2o") || type.contains("scg") || type.contains("oto")) {

      int ll = local_ids.Length(), dl = dep_ids.Length();
      if (ll >= dl) {
        int ratio = (int) Math.floor((ll * 1.0) / (dl * 1.0));
        int dst = (task - local_ids.begin) / ratio + dep_ids.begin;
        if (dst > dep_ids.end)
          dst = dep_ids.end;

        return new Interval(dst, dst);
      } else {
        int ratio = (int) Math.floor((dl * 1.0) / (ll * 1.0));

        int dst_b = (task - local_ids.begin) * ratio + dep_ids.begin;
        int dst_e = dst_b + ratio - 1;

        if (task == local_ids.end)
          dst_e = dep_ids.end;

        return new Interval(dst_b, dst_e);
      }
    } else {
      System.err.println("Unknown dependency type");
      return null;
    }
  }
}
