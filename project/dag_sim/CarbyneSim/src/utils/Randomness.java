package carbyne.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Randomness {

  Random rand = null;

  public Randomness() {
    rand = new Random(230859);
  }

  private double pickRandomDouble() {
    return rand.nextDouble();
  }

  public double pickRandomDouble(double minV, double maxV) {
    return pickRandomDouble() * (maxV - minV) + minV;
  }

  public int pickRandomInt(int maxRange) { // 0 -- maxRange-1
    return pickRandomInt(0, maxRange - 1);
  }

  public int pickRandomInt(int minRange, int maxRange) { // both inclusive
    // range has to be the same size as # of ints needed
    int val = (int) Math.floor(pickRandomDouble() * (maxRange - minRange + 1))
        + minRange;

    /*
     * if (val == maxRange && maxRange != minRange) val -= 1;
     */

    assert (val >= minRange && val <= maxRange);
    return val;
  }

  // mean = 0, stdev = 1
  public double GetNormal() {
    // Use Box-Muller algorithm
    double u1 = pickRandomDouble();
    double u2 = pickRandomDouble();
    double r = Math.sqrt(-2.0 * Math.log(u1));
    double theta = 2.0 * Math.PI * u2;
    return r * Math.sin(theta);
  }

  public double GetNormalSample(double mean, double stdev) {
    return GetNormal() * stdev + mean;
  }

  public double GetExponentialSample(double mean) {
    //
    // Let X be U [0, 1]
    // note mean = 1/\lambda for exponential
    // Pr (Y < y ) = Pr (-1/\lambda logX < y ) = Pr ( X > exp(-\lambda y)) = 1 -
    // exp (-\lambda y)
    //
    // Hence Y = -1 * mean * log (X)
    //
    assert (mean > 0);
    return -1 * mean * Math.log(pickRandomDouble());
  }

  public double GetParetoSample(double shape_alpha, double scale) {
    //
    // Let X be U [0, 1]
    // note scale is x_m. cdf Pr(Y < y ) = 1 - (x_m/y)^\alpha = ... = Pr ( x_m/
    // X^{1/\alpha} < y)
    //
    // Hence Y = x_m/ Pow(X, 1/ \alpha)
    //
    // mean = \inf if alpha \leq 1; \alpha * scale / (\alpha - 1) otherwise
    // variance = \inf if alpha < 2; \alpha * scale * scale / (\alpha-1) /
    // (\alpha-1) / (\alpha-2)
    //
    assert (shape_alpha > 0 && scale > 0);
    return scale / Math.pow(pickRandomDouble(), 1.0 / shape_alpha);
  }

  public int[] GetRandomPermutation(int n) {
    return GetRandomPermutation(n, n);
  }

  // / <summary>
  // / Random permutation of 0 ... n-1
  // / Second parameter m is optional
  // / m \in [0, n]
  // / when specified it yields only m out of the n values
  // / </summary>
  // / <param name="n"></param>
  // / <param name="m"></param>
  // / <returns></returns>
  public int[] GetRandomPermutation(int n, int m) {
    assert (m >= 0 && m <= n);

    // return an array with a random permutation of integers 0, 1, ... n-1
    int[] retval = new int[m];

    List<Integer> allIndices = new ArrayList<Integer>();

    for (int i = 0; i < n; i++)
      allIndices.add(i);

    for (int i = 0; i < m; i++) {
      int pick = pickRandomInt(allIndices.size());
      retval[i] = allIndices.get(pick);

      allIndices.remove(pick);
    }

    return retval;
  }
}
