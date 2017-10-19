package carbyne.datastructures;

import carbyne.simulator.Main.Globals;
import carbyne.utils.Utils;

@SuppressWarnings("rawtypes")
public class Resources implements Comparable {

  public double[] resources;

  public Resources() {
    resources = new double[Globals.NUM_DIMENSIONS];
  }

  public Resources(double size) {
    resources = new double[Globals.NUM_DIMENSIONS];
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      resources[i] = Utils.round(size, 2);
    }
  }

  public Resources(Resources res) {
    resources = new double[Globals.NUM_DIMENSIONS];
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      resources[i] = Utils.round(res.resource(i), 2);
    }
  }

  public Resources(double[] res) {
    resources = new double[Globals.NUM_DIMENSIONS];
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      resources[i] = Utils.round(res[i], 2);
    }
  }

  public static double aggrResources(Resources res) {
    double aggr = 0;
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      aggr += res.resource(i);
    }
    return aggr;
  }

  public static double l2Norm(Resources res) {
    double l2Norm = 0;
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      l2Norm += Math.pow(res.resource(i), 2);
    }
    l2Norm = Math.sqrt(l2Norm);
    return l2Norm;
  }

  public static double normDouble(double number) {
    return Math.round(number * 100) / 100;
  }

  public static Resources add(Resources res, int val) {
    Resources addedRes = Resources.clone(res);
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      addedRes.resources[i] += val;
      addedRes.resources[i] = Utils.round(addedRes.resources[i], 2);
    }
    return addedRes;
  }

  // same as add operation, except we don't cap to 1.0
  public void sum(Resources res) {
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      resources[i] += res.resources[i];
      resources[i] = Utils.round(resources[i], 2);
    }
  }

  public void subtract(Resources res) {
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      resources[i] -= res.resources[i];
      resources[i] = Utils.round(Math.max(resources[i], 0), 2);
    }
  }

  public void subtract(double decr) {
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      resources[i] -= decr;
      resources[i] = Utils.round(Math.max(resources[i], 0), 2);
    }
  }

  public void multiply(double factor) {
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      resources[i] *= factor;
      //resources[i] = Utils.round(resources[i], 2);
    }
  }

  public void divide(double factor) {
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      resources[i] /= factor;
      //resources[i] = Utils.round(resources[i], 2);
    }
  }

  public void divide(Resources res) {
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      resources[i] /= res.resource(i);
      //resources[i] = Utils.round(resources[i], 2);
    }
  }

  public int resBottleneck() {
    int res_idx_b = -1;
    double max = Double.MIN_VALUE;
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      if (max < resources[i]) {
        max = resources[i];
        res_idx_b = i;
      }
    }
    return res_idx_b;
  }
  public double max() {
    double max = Double.MIN_VALUE;
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      max = Math.max(max, resources[i]);
    }
    return max;
  }

  public static Resources min(Resources a, Resources b) {
    if (a.greaterOrEqual(b))
      return Resources.clone(b);
    return Resources.clone(a);
  }

  public static Resources piecewiseMin(Resources a, Resources b) {
    Resources ret = new Resources();
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      ret.resources[i] = Math.min(a.resources[i], b.resources[i]);
    }
    return ret;
  }

  public static Resources subtract(Resources total, Resources decr) {
    Resources subtractedRes = new Resources(0.0);
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      subtractedRes.resources[i] = Utils.round(total.resources[i]
          - decr.resources[i], 2);
    }
    return subtractedRes;
  }

  public static Resources clone(Resources res) {
    Resources clonedRes = new Resources();
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      clonedRes.resources[i] = res.resources[i];
    }
    return clonedRes;
  }

  public double resource(int idx) {
    assert (idx >= 0 && idx < Globals.NUM_DIMENSIONS);
    return resources[idx];
  }

  public static Resources divide(Resources res, int factor) {
    assert (factor > 0);
    Resources normalizedRes = new Resources();
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      normalizedRes.resources[i] = Utils.round(res.resources[i] / factor, 2);
    }
    return normalizedRes;
  }

  public void copy(Resources res) {
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      resources[i] = res.resources[i];
    }
  }

  public static double dotProduct(Resources a, Resources b) {
    double score = 0;
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      if (a.resources[i] + (1 - b.resources[i]) > 1.0001) {
        return -1;
      } else {
        score += a.resources[i] * b.resources[i];
      }
    }
    return Utils.round(score, 2);
  }

  public boolean distinctNew(Resources res) {
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      if (resources[i] != res.resources[i])
        return true;
    }
    return false;
  }

  public boolean distinct(Resources res) {
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      if (resources[i] + .0001 < res.resources[i]
          || resources[i] > res.resources[i] + .0001)
        return true;
    }
    return false;
  }

  public void normalize() {
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      if (resources[i] < 0)
        resources[i] = 0;
    }
  }

  public boolean greaterOrEqual(Resources res) {
    // every dimension should be at greater or equal
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      if (resources[i] < res.resource(i))
        return false;
    }
    return true;
  }

  public boolean greater(Resources res) {
    // at least equal in all dimensions
    // at least greater in one dimension
    if (!greaterOrEqual(res)) {
      return false;
    }

    // check that at least one dimension is bigger
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      if (resources[i] > res.resource(i)) {
        return true;
      }
    }
    return false;
  }

  public boolean smaller(Resources res) {
    // at least equal in all dimensions
    // at least greater in one dimension
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      if (resources[i] > res.resource(i)) {
        return false;
      }
    }

    // check that at least one dimension is smaller
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      if (resources[i] < res.resource(i)) {
        return true;
      }
    }
    return false;
  }

  public boolean equal(Resources res) {
    // at least equal in all dimensions
    // at least greater in one dimension
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      if ((resources[i] > res.resource(i)) || (resources[i] < res.resource(i))) {
        return false;
      }
    }
    return true;
  }

  public int compareUnclearRes(Resources res) {
    double sum1 = Resources.aggrResources(this);
    double sum2 = Resources.aggrResources(res);
    if (sum2 > sum1)
      return 1;
    if (sum1 > sum2)
      return -1;
    return 0;
  }

  @Override
  public int compareTo(Object o) {
    if (((Resources) o).greater(this)) {
      return 1;
    } else if (((Resources) o).smaller(this)) {
      return -1;
    } else if (((Resources) o).equal(this)) {
      return 0;
    } else {
      return compareUnclearRes((Resources) o);
    }
  }

  @Override
  public String toString() {
    String output = "Resources: [";
    for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
      output += resources[i] + " ";
    }
    output += "]";
    return output;
  }
}
