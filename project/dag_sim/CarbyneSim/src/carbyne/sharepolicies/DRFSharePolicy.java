package carbyne.sharepolicies;

import carbyne.cluster.Cluster;
import carbyne.datastructures.BaseDag;
import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;
import carbyne.simulator.Main.Globals;
import carbyne.simulator.Simulator;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class DRFSharePolicy extends SharePolicy {
  private static Logger LOG = Logger.getLogger(DRFSharePolicy.class.getName());
  Resources clusterTotCapacity = null;

  public DRFSharePolicy(String policyName, Cluster cluster) {
    super(policyName);
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
    LOG.info("start DRF schedule. Number of running jobs:" + Simulator.runningJobs.size());
    if (Simulator.runningJobs.size() == 0) {
      return;
    }

    Resources rsrcAlloc = new Resources();
    Map<Integer, Double> dominantShares = new HashMap<>();
    Map<Integer, Resources> resDemandsDags = new HashMap<>();
    Map<Integer, Resources> resGivenToDags = new HashMap<>();
    for (BaseDag job : Simulator.runningJobs) {
      resGivenToDags.put(job.dagId, new Resources());
      dominantShares.put(job.dagId, 0.0);
      Resources avgResDemandDag = ((StageDag) job).totalResourceDemand();
      avgResDemandDag.divide(((StageDag) job).numTotalTasks());
      resDemandsDags.put(job.dagId, avgResDemandDag);
      LOG.fine("Dag " + job.dagId + " resouce demands: " + resDemandsDags.get(job.dagId));
    }

    boolean stop = false;
    int minIdx = 0;
    double minShare = 1.1;
    while (!stop) {
      // pick user i with lowest dominiant share s_i
      minIdx = 0;
      minShare = 1.1;
      for (Map.Entry<Integer, Double> entry: dominantShares.entrySet()) {
        if (entry.getValue() < minShare) { 
          minIdx = entry.getKey(); 
          minShare = entry.getValue();
        }
      }
      Resources rsrcDemands = resDemandsDags.get(minIdx);
      rsrcAlloc.sum(rsrcDemands);

      if (clusterTotCapacity.greaterOrEqual(rsrcAlloc)) {
        Resources resGiven = resGivenToDags.get(minIdx);
        resGiven.sum(rsrcDemands);
        double s = -0.01;
        for (int j = 0; j < Globals.NUM_DIMENSIONS; j++) {
          s = Math.max(s, resGiven.resource(j) / clusterTotCapacity.resource(j));
        }
        dominantShares.put(minIdx, s);
      } else {
        stop = true;
      }
    }
    
    /* for (BaseDag job : Simulator.runningJobs) {
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
      LOG.fine("Allocated to job:" + job.dagId + " share:"
        + job.rsrcQuota);

      // System.out.println("Allocated to job:" + job.dagId + " share:"
      // + job.rsrcQuota);
    } */
    for (BaseDag job : Simulator.runningJobs) {
      job.rsrcQuota = resGivenToDags.get(job.dagId);
      LOG.fine("Allocated to job:" + job.dagId + " share:"
        + job.rsrcQuota);
    }
  }
}
