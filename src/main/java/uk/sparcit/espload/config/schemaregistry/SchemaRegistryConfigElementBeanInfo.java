package uk.sparcit.espload.config.schemaregistry;

import static uk.sparcit.espload.config.schemaregistry.DefaultPropertiesHelper.DEFAULTS;

import java.beans.PropertyDescriptor;
import uk.sparcit.espload.input.avro.SchemaRegistryConfigPropertyEditor;
import uk.sparcit.espload.model.PropertyMapping;
import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jmeter.testbeans.gui.TableEditor;
import org.apache.jmeter.testbeans.gui.TextAreaEditor;
import org.apache.jmeter.testbeans.gui.TypeEditor;
import uk.sparcit.espload.util.SchemaRegistryKeyHelper;

public class SchemaRegistryConfigElementBeanInfo extends BeanInfoSupport {

    private static final String SCHEMA_REGISTRY_URL = "schemaRegistryUrl";

    private static final String SCHEMA_REGISTRY_PROPERTIES = "schemaRegistryProperties";

    private static final String LIST_OF_SCHEMAS = "listofSchemas";

    public SchemaRegistryConfigElementBeanInfo() {

        super(SchemaRegistryConfigElement.class);

        createPropertyGroup("schema_registry_config", new String[]{
            SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_PROPERTIES, LIST_OF_SCHEMAS
        });

        PropertyDescriptor schemaRegistryUrl = property(SCHEMA_REGISTRY_URL);
        schemaRegistryUrl.setPropertyEditorClass(SchemaRegistryConfigPropertyEditor.class);
        schemaRegistryUrl.setValue(NOT_UNDEFINED, Boolean.TRUE);
        schemaRegistryUrl.setValue(DEFAULT, SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL_DEFAULT);
        schemaRegistryUrl.setValue(NOT_EXPRESSION, Boolean.FALSE);

        TypeEditor tableEditor = TypeEditor.TableEditor;
        PropertyDescriptor tableProperties = property(SCHEMA_REGISTRY_PROPERTIES, tableEditor);
        tableProperties.setValue(TableEditor.CLASSNAME, PropertyMapping.class.getName());
        tableProperties.setValue(TableEditor.HEADERS, new String[]{"Property Name", "Property Value"});
        tableProperties
            .setValue(TableEditor.OBJECT_PROPERTIES, new String[]{PropertyMapping.PROPERTY_NAME, PropertyMapping.PROPERTY_VALUE});
        tableProperties.setValue(DEFAULT, DEFAULTS);
        tableProperties.setValue(NOT_UNDEFINED, Boolean.TRUE);
        tableProperties.setValue(NOT_EXPRESSION, Boolean.FALSE);

        PropertyDescriptor schemaList = property(LIST_OF_SCHEMAS);
        schemaList.setPropertyEditorClass(TextAreaEditor.class);
        schemaList.setValue(MULTILINE, Boolean.TRUE);


    }
}
