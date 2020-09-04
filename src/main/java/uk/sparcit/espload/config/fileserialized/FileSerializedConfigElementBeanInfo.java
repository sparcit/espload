package uk.sparcit.espload.config.fileserialized;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;

import uk.sparcit.espload.input.avro.FileSubjectPropertyEditor;
import uk.sparcit.espload.input.avro.SchemaConverterPropertyEditor;
import uk.sparcit.espload.model.FieldValueMapping;
import uk.sparcit.espload.util.PropsKeysHelper;
import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jmeter.testbeans.gui.TableEditor;
import org.apache.jmeter.testbeans.gui.TypeEditor;

public class FileSerializedConfigElementBeanInfo extends BeanInfoSupport {

  private static final String KEYORVALUE = "keyorvalue";
  private static final String AVRO_SUBJECT = "avroSubject";
  private static final String SCHEMA_PROPERTIES = "schemaProperties";
  private static final String AVRO_SCHEMA = "avroSchema";

  public FileSerializedConfigElementBeanInfo() {

    super(FileSerializedConfigElement.class);

    createPropertyGroup("file_serialized_load_generator", new String[]{
            KEYORVALUE, AVRO_SUBJECT, SCHEMA_PROPERTIES, AVRO_SCHEMA
    });

    String [] arr = {PropsKeysHelper.KEYORVALUE.Value.toString(), PropsKeysHelper.KEYORVALUE.Key.toString()};
//    String[] stringArray = Arrays.copyOf(PropsKeysHelper.KEYORVALUE.values(), PropsKeysHelper.KEYORVALUE.values().length, String[].class);
    PropertyDescriptor placeHolderProps = property(KEYORVALUE,TypeEditor.ComboStringEditor);
    placeHolderProps.setValue(TAGS, arr);
    placeHolderProps.setValue(NOT_UNDEFINED, Boolean.TRUE);
    placeHolderProps.setValue(DEFAULT, PropsKeysHelper.KEYORVALUE.Value.toString());
    placeHolderProps.setValue(NOT_EXPRESSION, Boolean.TRUE);

    PropertyDescriptor subjectNameProps = property(AVRO_SUBJECT);
    subjectNameProps.setPropertyEditorClass(FileSubjectPropertyEditor.class);
    subjectNameProps.setValue(NOT_UNDEFINED, Boolean.TRUE);
    subjectNameProps.setValue(DEFAULT, "<subject name>");
    subjectNameProps.setValue(NOT_EXPRESSION, Boolean.FALSE);

    PropertyDescriptor avroSchemaProps = property(AVRO_SCHEMA);
    avroSchemaProps.setPropertyEditorClass(SchemaConverterPropertyEditor.class);
    avroSchemaProps.setValue(NOT_UNDEFINED, Boolean.TRUE);
    avroSchemaProps.setValue(DEFAULT, "");
    avroSchemaProps.setValue(NOT_EXPRESSION, Boolean.FALSE);
    avroSchemaProps.setValue(MULTILINE,Boolean.TRUE);

    TypeEditor tableEditor = TypeEditor.TableEditor;
    PropertyDescriptor tableProperties = property(SCHEMA_PROPERTIES, tableEditor);
    tableProperties.setValue(TableEditor.CLASSNAME, FieldValueMapping.class.getName());
    tableProperties.setValue(TableEditor.HEADERS,
        new String[]{
            "Field Name",
            "Field Type",
            "Field Length",
            "Field Values List"
        });
    tableProperties.setValue(TableEditor.OBJECT_PROPERTIES,
        new String[]{
            FieldValueMapping.FIELD_NAME,
            FieldValueMapping.FIELD_TYPE,
            FieldValueMapping.VALUE_LENGTH,
            FieldValueMapping.FIELD_VALUES_LIST
        });
    tableProperties.setValue(DEFAULT, new ArrayList<>());
    tableProperties.setValue(NOT_UNDEFINED, Boolean.TRUE);
  }
}
