package carbyne.sharepolicies;

import carbyne.cluster.Cluster;
import carbyne.datastructures.BaseDag;
import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;
import carbyne.simulator.Simulator;

public class SJFSharePolicy extends SharePolicy {

  public SJFSharePolicy(String policyName) {
    super(policyName);
  }

  // in this policy, give all the resources to the job with min(SRTF)
  @Override
  public void computeResShare(Cluster cluster) {
    if (Simulator.runningJobs.size() == 0) {
      return;
    }

    // sort the jobs in SRTF order
    // give the entire cluster share to the job with smallest SRTF
    double shortestJobVal = Double.MAX_VALUE;
    int shortestJobId = -1;
    for (BaseDag job : Simulator.runningJobs) {
      double jobSrtf = ((StageDag) job).srtfScore();
      if (jobSrtf < shortestJobVal) {
        shortestJobVal = jobSrtf;
        shortestJobId = job.dagId;
      }
    }

    assert (shortestJobVal != Double.MAX_VALUE);
    for (BaseDag job : Simulator.runningJobs) {
      if (job.dagId == shortestJobId) {
        job.rsrcQuota = Resources.clone(cluster
            .getClusterMaxResAlloc());
      } else {
        job.rsrcQuota = new Resources(0.0);
      }
      // System.out.println("Allocated to job:" + job.dagId + " share:"
      // + job.rsrcQuota);
    }
  }
}
