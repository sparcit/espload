package uk.sparcit.espload.loadgen;

import java.util.List;
import uk.sparcit.espload.model.FieldValueMapping;
import uk.sparcit.espload.serializer.EnrichedRecord;

public interface BaseLoadGenerator {

    void setUpGeneratorFromRegistry(String avroSchemaName, List<FieldValueMapping> fieldExprMappings);

    void setUpGenerator(String schema, List<FieldValueMapping> fieldExprMappings);

    EnrichedRecord nextMessage();
}
