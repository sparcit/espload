package net.coru.kloadgen.config.fileserialized;

import java.util.List;

import kafka.utils.Implicits;
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

@Getter
@Setter
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class FileSerializedConfigElement  extends ConfigTestElement implements TestBean, LoopIterationListener {

//  private KEYORVALUE keyorvalue;
  private String keyorvalue;
  private String avroSubject;
  private List<FieldValueMapping> schemaProperties;
  private String avroSchema;


  @Override
  public void iterationStart(LoopIterationEvent loopIterationEvent) {

    JMeterVariables variables = JMeterContextService.getContext().getVariables();
    if (keyorvalue.equals(KEYORVALUE.Key.toString())){
      variables.putObject(KEY_SCHEMA, avroSchema);
      variables.putObject(KEY_SCHEMA_PROPERTIES, schemaProperties);
    }
    else {
      variables.putObject(AVRO_SCHEMA, avroSchema);
      variables.putObject(SCHEMA_PROPERTIES, schemaProperties);
    }
  }

}
