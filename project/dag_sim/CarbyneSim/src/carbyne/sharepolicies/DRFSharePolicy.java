package carbyne.sharepolicies;

import carbyne.cluster.Cluster;
import carbyne.datastructures.BaseDag;
import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;
import carbyne.simulator.Main.Globals;
import carbyne.simulator.Simulator;

import java.util.HashMap;
import java.util.Map;

public class DRFSharePolicy extends SharePolicy {

  Map<Integer, Resources> resDemandsDags = null;
  Resources clusterTotCapacity = null;

  public DRFSharePolicy(String policyName, Cluster cluster) {
    super(policyName);
    resDemandsDags = new HashMap<Integer, Resources>();
    clusterTotCapacity = cluster.getClusterMaxResAlloc();
  }

  // FairShare = DRF implementation
  // implementation idea:
  // 1. for every job, compute it's total resource demand vector
  // 2. for every job's resource demand vector, normalize every dimension
  //    to the total capacity of the cluster
  // 3. scale a job's resource demand vector if any dimension is larger than
  //    total capacity of the cluster
  // 4. sum up across every dimension across all the resource demand vectors
  // 5. inverse the max sum across dimensions 1 / max_sum
  // 6. the DRF allocation for every job is computed:
  // ResourceDemandVector * 1 / max_sum
  @Override
  public void computeResShare(Cluster cluster) {
    if (Simulator.runningJobs.size() == 0) {
      return;
    }

    for (BaseDag job : Simulator.runningJobs) {
      if (!resDemandsDags.containsKey(job.dagId)) {
        // 1. compute it's avg. resource demand vector it not already computed
        Resources avgResDemandDag = ((StageDag) job).totalResourceDemand();
        avgResDemandDag.divide(job.allTasks().size());

        // 2. normalize every dimension to the total capacity of the cluster
        avgResDemandDag.divide(clusterTotCapacity);

        // 3. scale the resource demand vector to the max resource
        avgResDemandDag.divide(avgResDemandDag.max());
        resDemandsDags.put(job.dagId, avgResDemandDag);
      }
    }

    // 4. sum it up across every dimension
    Resources sumDemandsRunDags = new Resources(0.0);
    for (BaseDag job : Simulator.runningJobs) {
      sumDemandsRunDags.sum(resDemandsDags.get(job.dagId));
    }

    // 5. find the max sum
    double drfShare = Globals.MACHINE_MAX_RESOURCE / sumDemandsRunDags.max();

    // 6. update the resource quota for every running job
    for (BaseDag job : Simulator.runningJobs) {
      Resources jobDRFQuota = Resources.clone(resDemandsDags.get(job.dagId));
      jobDRFQuota.multiply(drfShare);
      job.rsrcQuota = jobDRFQuota;
      // System.out.println("Allocated to job:" + job.dagId + " share:"
      // + job.rsrcQuota);
    }
  }
}
