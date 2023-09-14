
package com.jdvn.setl.geos.processors.gss;

import static org.apache.nifi.util.db.JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION;
import static org.apache.nifi.util.db.JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import com.jdvn.setl.geos.processors.db.AbstractQueryGSSTable;
import com.jdvn.setl.geos.processors.db.JdbcCommon;
import com.jdvn.setl.geos.processors.db.RecordSqlWriter;
import com.jdvn.setl.geos.processors.db.SqlWriter;




@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"layer", "list", "GSS", "feature table", "geo database"})
@SeeAlso({ListGSSTables.class})
@CapabilityDescription("Get geospatial data from GSS")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a query on the specified table, the maximum values for "
        + "the specified column(s) will be retained for use in future executions of the query. This allows the Processor "
        + "to fetch only those records that have max values greater than the retained values. This can be used for "
        + "incremental fetching, fetching of newly added rows, etc. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation")
@WritesAttributes({
        @WritesAttribute(attribute = "tablename", description="Name of the table being queried"),
        @WritesAttribute(attribute = "querydbtable.row.count", description="The number of rows selected by the query"),
        @WritesAttribute(attribute="fragment.identifier", description="If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet. If Output Batch Size is set, then this "
                + "attribute will not be populated."),
        @WritesAttribute(attribute="fragment.index", description="If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "maxvalue.*", description = "Each attribute contains the observed maximum value of a specified 'Maximum-value Column'. The "
                + "suffix of the attribute is the name of the column. If Output Batch Size is set, then this attribute will not be populated."),
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer."),
        @WritesAttribute(attribute = "record.count", description = "The number of records output by the Record Writer.")
})
@DynamicProperty(name = "initial.maxvalue.<max_value_column>", value = "Initial maximum value for the specified column",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY, description = "Specifies an initial max value for max value column(s). Properties should "
        + "be added in the format `initial.maxvalue.<max_value_column>`. This value is only used the first time the table is accessed (when a Maximum Value Column is specified).")
@PrimaryNodeOnly
public class FetchGSS extends AbstractQueryGSSTable {

    public static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("qdbtr-record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing results to a FlowFile. The Record Writer may use Inherit Schema to emulate the inferred schema behavior, i.e. "
                    + "an explicit schema need not be defined in the writer, and will be supplied by the same logic used to infer the schema from the column types.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor NORMALIZE_NAMES = new PropertyDescriptor.Builder()
            .name("qdbtr-normalize")
            .displayName("Normalize Table/Column Names")
            .description("Whether to change characters in column names when creating the output schema. For example, colons and periods will be changed to underscores.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public FetchGSS() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(GSS_SERVICE);
        pds.add(DB_TYPE);
        pds.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(TABLE_NAME)
                .description("The name of the database table to be queried. When a custom query is used, this property is used to alias the query and appears as an attribute on the FlowFile.")
                .build());
        pds.add(COLUMN_NAMES);
        pds.add(WHERE_CLAUSE);
        pds.add(SQL_QUERY);
        pds.add(RECORD_WRITER_FACTORY);
        pds.add(MAX_ROWS_PER_FLOW_FILE);
        pds.add(OUTPUT_BATCH_SIZE);
        pds.add(MAX_FRAGMENTS);
        pds.add(NORMALIZE_NAMES);
        pds.add(VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION);
        pds.add(VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE);
        pds.add(GENERATE_EVENT_TRACKERS);
        

        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    protected SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context) {
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES).asBoolean();
        final Boolean useAvroLogicalTypes = true;
        final Integer defaultPrecision = context.getProperty(VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION).evaluateAttributeExpressions().asInteger();
        final Integer defaultScale = context.getProperty(VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE).evaluateAttributeExpressions().asInteger();

        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions.builder()
                .convertNames(convertNamesForAvro)
                .useLogicalTypes(useAvroLogicalTypes)
                .defaultPrecision(defaultPrecision)
                .defaultScale(defaultScale)
                .build();
        final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);

        return new RecordSqlWriter(recordSetWriterFactory, options, maxRowsPerFlowFile, Collections.emptyMap());
    }
}
