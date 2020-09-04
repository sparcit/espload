package uk.sparcit.espload.config.avroserialized;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import uk.sparcit.espload.model.FieldValueMapping;
import uk.sparcit.espload.util.PropsKeysHelper;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.engine.event.LoopIterationEvent;
import org.apache.jmeter.engine.event.LoopIterationListener;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

@Getter
@Setter
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class AvroSerializedConfigElement extends ConfigTestElement implements TestBean, LoopIterationListener {

  private String keyorvalue;
  private String avroSubject;
  private List<FieldValueMapping> schemaProperties;

  @Override
  public void iterationStart(LoopIterationEvent loopIterationEvent) {

      JMeterVariables variables = JMeterContextService.getContext().getVariables();
      if (keyorvalue.equals(PropsKeysHelper.KEYORVALUE.Key.toString())){
          variables.putObject(PropsKeysHelper.KEY_SUBJECT_NAME, avroSubject);
          variables.putObject(PropsKeysHelper.KEY_SCHEMA_PROPERTIES, schemaProperties);
      }
      else {
          variables.putObject(PropsKeysHelper.AVRO_SUBJECT_NAME, avroSubject);
          variables.putObject(PropsKeysHelper.SCHEMA_PROPERTIES, schemaProperties);
      }
  }

}
