package carbyne.schedpolicies;

import carbyne.cluster.Cluster;
import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;

public abstract class SchedPolicy {

  public Cluster cluster;

  public SchedPolicy(Cluster _cluster) {
    cluster = _cluster;
  }

  public abstract void schedule(StageDag dag);

  public abstract double planSchedule(StageDag dag, Resources leftOverResources);
}
