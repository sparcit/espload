package uk.sparcit.espload.config.avroserialized;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import uk.sparcit.espload.input.avro.AvroSubjectPropertyEditor;
import uk.sparcit.espload.model.FieldValueMapping;
import uk.sparcit.espload.util.PropsKeysHelper;
import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jmeter.testbeans.gui.TableEditor;
import org.apache.jmeter.testbeans.gui.TypeEditor;

public class AvroSerializedConfigElementBeanInfo extends BeanInfoSupport {

    private static final String KEYORVALUE = "keyorvalue";
    private static final String AVRO_SUBJECT = "avroSubject";
    private static final String SCHEMA_PROPERTIES = "schemaProperties";

    public AvroSerializedConfigElementBeanInfo() {

        super(AvroSerializedConfigElement.class);

        createPropertyGroup("avro_serialized_load_generator", new String[] {
                KEYORVALUE, AVRO_SUBJECT, SCHEMA_PROPERTIES
        });

        String [] arr = {PropsKeysHelper.KEYORVALUE.Value.toString(), PropsKeysHelper.KEYORVALUE.Key.toString()};
//    String[] stringArray = Arrays.copyOf(PropsKeysHelper.KEYORVALUE.values(), PropsKeysHelper.KEYORVALUE.values().length, String[].class);
        PropertyDescriptor placeHolderProps = property(KEYORVALUE,TypeEditor.ComboStringEditor);
        placeHolderProps.setValue(TAGS, arr);
        placeHolderProps.setValue(NOT_UNDEFINED, Boolean.TRUE);
        placeHolderProps.setValue(DEFAULT, PropsKeysHelper.KEYORVALUE.Value.toString());
        placeHolderProps.setValue(NOT_EXPRESSION, Boolean.TRUE);

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

        PropertyDescriptor subjectNameProps = property(AVRO_SUBJECT);
        subjectNameProps.setPropertyEditorClass(AvroSubjectPropertyEditor.class);
        subjectNameProps.setValue(NOT_UNDEFINED, Boolean.TRUE);
        subjectNameProps.setValue(NOT_EXPRESSION, Boolean.FALSE);
    }
}
