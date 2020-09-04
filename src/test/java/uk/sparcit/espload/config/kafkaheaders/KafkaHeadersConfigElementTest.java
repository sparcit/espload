package uk.sparcit.espload.config.kafkaheaders;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import org.apache.jmeter.threads.JMeterContextService;
import org.junit.jupiter.api.Test;
import uk.sparcit.espload.util.ProducerKeysHelper;

class KafkaHeadersConfigElementTest {

  @Test
  public void testIterateStart() {
    KafkaHeadersConfigElement kafkaHeadersConfigElement = new KafkaHeadersConfigElement();
    kafkaHeadersConfigElement.setKafkaHeaders(Collections.emptyList());
    kafkaHeadersConfigElement.iterationStart(null);

    Object kafkaHeaders = JMeterContextService.getContext().getSamplerContext().get(ProducerKeysHelper.KAFKA_HEADERS);

    assertThat(kafkaHeaders).isNotNull();
    assertThat(kafkaHeaders).isInstanceOf(List.class);
  }
}