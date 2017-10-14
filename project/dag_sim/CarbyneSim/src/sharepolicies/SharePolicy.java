package carbyne.sharepolicies;

public abstract class SharePolicy {

  public String sharingPolicyName;

  public SharePolicy(String policyName) {
    sharingPolicyName = policyName;
  }

  // recompute the resource share allocated for every job
  public void computeResShare() {
  }

  public void packTasks() {}
}
