package net.coru.kloadgen.config.avroserialized;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.coru.kloadgen.model.FieldValueMapping;
import net.coru.kloadgen.util.PropsKeysHelper;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.engine.event.LoopIterationEvent;
import org.apache.jmeter.engine.event.LoopIterationListener;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

import static net.coru.kloadgen.util.PropsKeysHelper.*;
import static net.coru.kloadgen.util.PropsKeysHelper.KEY_SCHEMA_PROPERTIES;

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
          variables.putObject(KEY_SUBJECT_NAME, avroSubject);
          variables.putObject(KEY_SCHEMA_PROPERTIES, schemaProperties);
      }
      else {
          variables.putObject(AVRO_SUBJECT_NAME, avroSubject);
          variables.putObject(SCHEMA_PROPERTIES, schemaProperties);
      }
  }

}
