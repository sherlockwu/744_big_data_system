package carbyne.F2;

import java.util.Queue;

class ExecuteService {
  public ExecuteService() {}

  public void receiveReadyEvents(Queue<ReadyEvent> readyEventQueue) {}

  public void schedule() {}

  public void emitSpillEvents(Queue<SpillEvent> spillEventQueue) {}
}
