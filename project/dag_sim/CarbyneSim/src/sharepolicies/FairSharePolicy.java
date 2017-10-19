package carbyne.sharepolicies;

import carbyne.datastructures.BaseDag;
import carbyne.datastructures.Resources;
import carbyne.simulator.Simulator;

public class FairSharePolicy extends SharePolicy {

  Resources clusterTotCapacity = null;

  public FairSharePolicy(String policyName) {
    super(policyName);
    clusterTotCapacity = Simulator.cluster.getClusterMaxResAlloc();
  }

  // FairShare = 1 / N across all dimensions
  // N - total number of running jobs
  @Override
  public void computeResShare() {
    int numJobsRunning = Simulator.runningJobs.size();
    if (numJobsRunning == 0) {
      return;
    }

    Resources quotaRsrcShare = Resources.divide(clusterTotCapacity,
        numJobsRunning);

    // update the resourceShareAllocated for every running job
    for (BaseDag job : Simulator.runningJobs) {
      job.rsrcQuota = quotaRsrcShare;
      // System.out.println("Allocated to job:" + job.dagId + " share:"
      // + job.rsrcQuota);
    }
  }
}
