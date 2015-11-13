import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zs on 15/2/3.
 */
public class MockData {

  private Properties p = null;
  private Producer<String, byte[]> producer = null;
  private String topic = null;
  private String brokerList = null;

  public MockData(String brokerList, String topic) {
    this.brokerList = brokerList;
    this.topic = topic;

    loadProperties();
  }

  private void process(int count) {
    try {
      for (int i = 1; i <= count; i++) {
        KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>(topic, Integer.toString(i).getBytes());
        producer.send(message);
      }
    } finally {
      producer.close();
    }
  }

  private Producer<String, byte[]> createProducer(Properties p) {
    return new Producer<String, byte[]>(new ProducerConfig(p));
  }

  private void loadProperties() {
    InputStream is = MockData.class.getClassLoader().getResourceAsStream("streaming.properties");
    p = new Properties();
    try {
      p.load(is);
      p.put("metadata.broker.list", brokerList);
      producer = createProducer(p);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void main(String[] args) {
    String brokerList = args[0];
    String topic = args[1];
    int count = Integer.parseInt(args[2]);

    MockData mockData = new MockData(brokerList, topic);
    mockData.process(count);
  }
}
