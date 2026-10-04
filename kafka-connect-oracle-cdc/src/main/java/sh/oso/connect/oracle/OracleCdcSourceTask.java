package sh.oso.connect.oracle;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single capture task. The engine wiring lands with the vertical slice (plan increment P1-10);
 * until then the task validates its configuration and idles so the plugin can be installed and
 * listed by a Connect worker.
 */
public class OracleCdcSourceTask extends SourceTask {

  private static final Logger LOG = LoggerFactory.getLogger(OracleCdcSourceTask.class);

  private volatile boolean running;

  @Override
  public String version() {
    return Version.VERSION;
  }

  @Override
  public void start(Map<String, String> props) {
    OracleCdcSourceConnectorConfig config = new OracleCdcSourceConnectorConfig(props);
    running = true;
    LOG.info(
        "Task started for topic prefix {} (capture engine not wired yet)", config.topicPrefix());
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    if (!running) {
      return null;
    }
    Thread.sleep(1000);
    return null;
  }

  @Override
  public void stop() {
    running = false;
  }
}
