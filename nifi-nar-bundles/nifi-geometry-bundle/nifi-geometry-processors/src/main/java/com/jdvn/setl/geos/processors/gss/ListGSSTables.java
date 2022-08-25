/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jdvn.setl.geos.processors.gss;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import com.cci.gss.jdbc.driver.IGSSConnection;
import com.cci.gss.jdbc.driver.IGSSDatabaseMetaData;
import com.cci.gss.jdbc.driver.IGSSStatement;
import com.jdvn.setl.geos.gss.DbmsType;
import com.jdvn.setl.geos.gss.GSSService;
import com.jdvn.setl.geos.gss.PropertyConstants;

/**
 * A processor to retrieve a list of feature tables (and their metadata) from a GSS database connection
 */
@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"layer", "list", "GSS", "feature table", "geo database"})
@CapabilityDescription("Generates a set of flow files, each containing attributes corresponding to metadata about a feature table from a GSS database connection. Once "
        + "metadata about a table has been fetched, it will not be fetched again until the Refresh Interval (if set) has elapsed, or until state has been "
        + "manually cleared.")
@WritesAttributes({
        @WritesAttribute(attribute = "db.table.name", description = "Contains the name of a database table from the connection"),
        @WritesAttribute(attribute = "db.table.catalog", description = "Contains the name of the catalog to which the table belongs (may be null)"),
        @WritesAttribute(attribute = "db.table.schema", description = "Contains the name of the schema to which the table belongs (may be null)"),
        @WritesAttribute(attribute = "db.table.fullname", description = "Contains the fully-qualifed table name (possibly including catalog, schema, etc.)"),
        @WritesAttribute(attribute = "db.table.type",
                description = "Contains the type of the database table from the connection. Typical types are \"TABLE\", \"VIEW\", \"SYSTEM TABLE\", "
                        + "\"GLOBAL TEMPORARY\", \"LOCAL TEMPORARY\", \"ALIAS\", \"SYNONYM\""),
        @WritesAttribute(attribute = "db.table.remarks", description = "Contains the name of a database table from the connection"),
        @WritesAttribute(attribute = "db.table.count", description = "Contains the number of rows in the table")
})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of tables, the timestamp of the query is stored. "
        + "This allows the Processor to not re-list tables the next time that the Processor is run. Specifying the refresh interval in the processor properties will "
        + "indicate that when the processor detects the interval has elapsed, the state will be reset and tables will be re-listed as a result. "
        + "This processor is meant to be run on the primary node only.")
public class ListGSSTables extends AbstractProcessor {

    // Attribute names
    public static final String DB_TABLE_NAME = "db.table.name";
    public static final String DB_TABLE_CATALOG = "db.table.catalog";
    public static final String DB_TABLE_SCHEMA = "db.table.schema";
    public static final String DB_TABLE_FULLNAME = "db.table.fullname";
    public static final String DB_TABLE_TYPE = "db.table.type";
    public static final String DB_TABLE_REMARKS = "db.table.remarks";
    public static final String DB_TABLE_COUNT = "db.table.count";

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received are routed to success")
            .build();

    // Property descriptors
    public static final PropertyDescriptor GSS_STORE = new PropertyDescriptor.Builder()
            .name("list-db-tables-db-connection")
            .displayName("GSS Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to GSS database")
            .required(true)
            .identifiesControllerService(GSSService.class)
            .build();
    public static final AllowableValue LAYER = new AllowableValue("Layers", "Layers", "Loads GIS Layers in GSS store");
    public static final AllowableValue TABLE = new AllowableValue("Tables", "Tables", "Loads all Tables");
    public static final AllowableValue VIEW = new AllowableValue("Views", "Views", "Loads all Views");
    public static final AllowableValue TABLE_AND_VIEW = new AllowableValue("Tables & Views", "Tables & Views", "Loads all Tables and Views");

    public static final PropertyDescriptor DATA_TYPE = new PropertyDescriptor.Builder()
            .name("gss-type-data-to-load")
            .displayName("Data type")
            .description("Which type of metadata to fetch. It could be Layers, Tables or Views")
            .required(true)
            .allowableValues(LAYER, TABLE, VIEW, TABLE_AND_VIEW)
            .defaultValue(TABLE_AND_VIEW.getValue())
            .build();
    
    public static final PropertyDescriptor INCLUDE_COUNT = new PropertyDescriptor.Builder()
            .name("list-db-include-count")
            .displayName("Include Count")
            .description("Whether to include the table's row count as a flow file attribute. This affects performance as a database query will be generated "
                    + "for each table in the retrieved list.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor REFRESH_INTERVAL = new PropertyDescriptor.Builder()
            .name("list-db-refresh-interval")
            .displayName("Refresh Interval")
            .description("The amount of time to elapse before resetting the processor state, thereby causing all current tables to be listed. "
                    + "During this interval, the processor may continue to run, but tables that have already been listed will not be re-listed. However new/added "
                    + "tables will be listed as the processor runs. A value of zero means the state will never be automatically reset, the user must "
                    + "Clear State manually.")
            .required(true)
            .defaultValue("0 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Record Writer to use for creating the listing. If not specified, one FlowFile will be created for each entity that is listed. If the Record Writer is specified, " +
            "all entities will be written to a single FlowFile instead of adding attributes to individual FlowFiles.")
        .required(false)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .build();


    private static final List<PropertyDescriptor> propertyDescriptors;
    private static final Set<Relationship> relationships;
	private static final Set<String> EXCLUSION_TABLES = new HashSet<String>(Arrays.asList(new String[] {
			"DUMMY4TEMP", "GEOMETRY_COLUMNS", "NETWORKS", "NETWORK_RELATIONS",
			"PATHREGISTRY", "PATHS", "PYRAMID_INFO", "RELREGISTRY", "RASTER_INFO",
			"SPATIAL_REF_SYS", "THEMES", "UPDATE_HISTORY", "GEOMETRY_LODS"
		})); 
	
	private DbmsType dbmsType;
	
    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        final List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(GSS_STORE);
        _propertyDescriptors.add(DATA_TYPE);
        _propertyDescriptors.add(INCLUDE_COUNT);
        _propertyDescriptors.add(RECORD_WRITER);
        _propertyDescriptors.add(REFRESH_INTERVAL);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final GSSService gssService = context.getProperty(GSS_STORE).asControllerService(GSSService.class);
        final String gssType = context.getProperty(DATA_TYPE).getValue();
        final boolean includeCount = context.getProperty(INCLUDE_COUNT).asBoolean();
        final long refreshInterval = context.getProperty(REFRESH_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);

        final StateMap stateMap;
        final Map<String, String> stateMapProperties;
        try {
            stateMap = session.getState(Scope.CLUSTER);
            stateMapProperties = new HashMap<>(stateMap.toMap());
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final TableListingWriter writer;
        if (writerFactory == null) {
            writer = new AttributeTableListingWriter(session);
        } else {
            writer = new RecordTableListingWriter(session, writerFactory, getLogger());
        }
        try (final IGSSConnection con = gssService.getConnection()) {
            writer.beginListing();

            IGSSDatabaseMetaData dbMetaData = (IGSSDatabaseMetaData) con.getMetaData();
            
            IGSSStatement stmt = (IGSSStatement) con.createStatement();
            ResultSet rs = null;
            
    		String catalog = null;
    		String scheme = null;
    		String dbmsPrefix = null;
    		
			String dbmsTypeString = con.getProperty(PropertyConstants.GSS_DBMS_TYPE);
			if (dbmsTypeString != null) {
				dbmsType = DbmsType.valueOf(dbmsTypeString);
			}
    		if (DbmsType.mssql == dbmsType) {
    			rs = stmt.executeNativeQuery("SELECT UPPER(DB_NAME())");
    			rs.next();
    			catalog = rs.getString(1);
    			rs.close();

    			rs = stmt.executeNativeQuery("SELECT SCHEMA_NAME()");
    			rs.next();
    			scheme = rs.getString(1);
    			rs.close();

    			dbmsPrefix = catalog + "." + scheme;
    		} else if (DbmsType.derby == dbmsType) {
    			dbmsPrefix = scheme = "APP";
    		} else {
    			rs = stmt.executeNativeQuery("SELECT USER FROM DUAL");
    			rs.next();
    			scheme = rs.getString(1);
    			rs.close();

    			dbmsPrefix = scheme;
    		}   
    		// // Get alls table and views
            String transitUri;
    		boolean hasGeometryLods = false;
    		final List<Map<String,String>> tableMetadata = new ArrayList<Map<String,String>>();
    		
    		String[] types = new String[] { "TABLE", "VIEW" };
    		if (gssType.equals("Tables"))
    			types = new String[] { "TABLE"};
    		else if (gssType.equals("Views"))
    			types = new String[] { "VIEW"};
    		
            try (ResultSet rs_ = dbMetaData.getTables(catalog, scheme, "%", types)) {
                try {
                    transitUri = dbMetaData.getURL();
                } catch (final SQLException sqle) {
                    transitUri = "<unknown>";
                }
                while (rs_.next()) {
        			String name = rs_.getString("TABLE_NAME").toUpperCase();
    				if (name.equals("GEOMETRY_LODS")) {
    					hasGeometryLods = true;
    				}
        			if (name.startsWith("BIN$") || name.startsWith("CREATE$JAVA$LOB$TABLE") || name.startsWith("MDRT_")
        					|| EXCLUSION_TABLES.contains(name)) {
        				continue;
        			}
                    final String tableCatalog = rs_.getString(1);
                    final String tableSchema = rs_.getString(2);
                    final String tableName = rs_.getString(3);
                    final String tableType = rs_.getString(4);
                    final String tableRemarks = rs_.getString(5);

                    // Build fully-qualified name
                    final String fqn = Stream.of(tableCatalog, tableSchema, tableName)
                        .filter(segment -> !StringUtils.isEmpty(segment))
                        .collect(Collectors.joining("."));

                    final Map<String, String> tableInformation = new HashMap<>();
                    if (tableCatalog != null) {
                        tableInformation.put(DB_TABLE_CATALOG, tableCatalog);
                    }
                    if (tableSchema != null) {
                        tableInformation.put(DB_TABLE_SCHEMA, tableSchema);
                    }
                    tableInformation.put(DB_TABLE_NAME, tableName);
                    tableInformation.put(DB_TABLE_FULLNAME, fqn);
                    tableInformation.put(DB_TABLE_TYPE, tableType);
                    if (tableRemarks != null) {
                        tableInformation.put(DB_TABLE_REMARKS, tableRemarks);
                    }                    
                    tableMetadata.add(tableInformation);
                }
                rs_.close();
            }
            dbMetaData.close();
       
			String operator = DbmsType.mssql == dbmsType ? "+" : "||";
			String prefix = DbmsType.mssql == dbmsType ? "GSS.dbo" : (DbmsType.derby == dbmsType ? "APP" : "GSS");
			
			StringBuffer sb = new StringBuffer();
			sb.append("SELECT 'X' ").append(operator);
			if (DbmsType.derby == dbmsType) {
				sb.append(" CHAR(THEME_ID)");
			}
			else {
				sb.append(" CAST(THEME_ID as VARCHAR(50))");
			}
			sb.append(" FROM ").append(prefix).append(".THEMES WHERE OWNER='").append(dbmsPrefix).append("'");
			sb.append(" UNION ");
			sb.append("SELECT THEME_NAME FROM ").append(prefix).append(".THEMES WHERE OWNER='").append(dbmsPrefix).append("'");
			sb.append(" UNION ");
			sb.append("SELECT G_TABLE_NAME FROM ").append(prefix).append(".GEOMETRY_COLUMNS WHERE G_TABLE_CATALOG='").append((catalog == null ? "DEFAULT" : catalog)).append("' AND G_TABLE_SCHEMA='").append(scheme).append("'");
			sb.append(" UNION ");
			sb.append("SELECT NODE_TABLE_NAME FROM ").append(prefix).append(".NETWORKS WHERE OWNER='").append(dbmsPrefix).append("'");
			sb.append(" UNION ");
			sb.append("SELECT LINK_TABLE_NAME FROM ").append(prefix).append(".NETWORKS WHERE OWNER='").append(dbmsPrefix).append("'");
			sb.append(" UNION ");
			sb.append("SELECT 'XL' ").append(operator);
			if (DbmsType.derby == dbmsType) {
				sb.append(" CHAR(NETWORK_ID)");
			}
			else {
				sb.append(" CAST(NETWORK_ID AS VARCHAR(50))");
			}
			sb.append(" FROM ").append(prefix).append(".NETWORKS WHERE OWNER='").append(dbmsPrefix).append("'");
			sb.append(" UNION ");
			sb.append("SELECT 'XN' ").append(operator);
			if (DbmsType.derby == dbmsType) {
				sb.append(" CHAR(NETWORK_ID)");
			}
			else {
				sb.append(" CAST(NETWORK_ID AS VARCHAR(50))");
			}
			sb.append(" FROM ").append(prefix).append(".NETWORKS WHERE OWNER='").append(dbmsPrefix).append("'");
			
			if (hasGeometryLods) {
				sb.append(" UNION ");
				sb.append("SELECT 'L' ").append(operator);
				if (DbmsType.derby == dbmsType) {
					sb.append(" CHAR(THEME_ID)");
				}
				else {
					sb.append(" CAST(THEME_ID as VARCHAR(50))");
				}
				sb.append(" FROM ").append(prefix).append(".GEOMETRY_LODS");
			}
			
			rs = stmt.executeNativeQuery(sb.toString());
			while (rs.next()) {
				String excludeTable = rs.getString(1);
				excludeTableFromList(tableMetadata,excludeTable);
			}
			rs.close();
			
			if (con.getGSSVersion().compareTo(3, 5, 0) >= 0) {
				sb.setLength(0);
				sb.append("SELECT RASTER_NAME FROM ").append(prefix).append(".RASTER_INFO");
				try {
					rs = stmt.executeNativeQuery(sb.toString());
					while (rs.next()) {
						String excludeTable = rs.getString("RASTER_NAME");
						excludeTableFromList(tableMetadata,excludeTable);
					}
					rs.close();
				}
				catch (SQLException e) {
				}
			}
			
			
			if (gssType.equals("Layers")) {
				// Get only Layers
	            final List<Map<String,String>> allLayerMetadata = new ArrayList<Map<String,String>>();
				for (String name : stmt.getAllLayerNames()) {
					int indexOfDot = name.lastIndexOf('.');
					if (indexOfDot != -1) {
						name = name.substring(indexOfDot + 1);
					}
					
					Iterator<Map<String, String>> allTableIterator = tableMetadata.iterator();
					while (allTableIterator.hasNext()) {
						final Map<String, String> tableInformation = allTableIterator.next();
						if (tableInformation.get(DB_TABLE_NAME).equals(name)) {
							allLayerMetadata.add(tableInformation);
							break;
						}
					}  
				}
				setMetadataInfoIntoList(con, logger, context, writer, allLayerMetadata, stateMapProperties, transitUri, refreshInterval, includeCount);
			}
			else {
				setMetadataInfoIntoList(con, logger, context, writer, tableMetadata, stateMapProperties, transitUri, refreshInterval, includeCount);
			}

			stmt.close();			
            writer.finishListing();
            session.replaceState(stateMap, stateMapProperties, Scope.CLUSTER);
            gssService.returnConnection(con);
            
        } catch (final SQLException | IOException | SchemaNotFoundException e) {
            writer.finishListingExceptionally(e);
            session.rollback();
            throw new ProcessException(e);
        }
        
    }
    
    private void excludeTableFromList(final List<Map<String, String>> tableMetadata, String excludeTable) {
    	
		Iterator<Map<String, String>> tableIterator = tableMetadata.iterator();
		
		while (tableIterator.hasNext()) {
			final Map<String, String> tableInformation = tableIterator.next();
			if (tableInformation.get(DB_TABLE_NAME) == excludeTable) {
				tableMetadata.remove(tableInformation);
			}
		}    	
    }
	public void setMetadataInfoIntoList(final IGSSConnection con, final ComponentLog logger,
			final ProcessContext context, final TableListingWriter writer,
			final List<Map<String, String>> tableMetadata, final Map<String, String> stateMapProperties,
			String transitUri, final long refreshInterval, final boolean includeCount) throws IOException {
		Iterator<Map<String, String>> tableIterator = tableMetadata.iterator();
		while (tableIterator.hasNext()) {

			final Map<String, String> tableInformation = tableIterator.next();
			String fqn = tableInformation.get(DB_TABLE_FULLNAME);

			final String lastTimestampForTable = stateMapProperties.get(fqn);
			boolean refreshTable = true;
			try {
				// Refresh state if the interval has elapsed
				long lastRefreshed = -1;
				final long currentTime = System.currentTimeMillis();
				if (!StringUtils.isEmpty(lastTimestampForTable)) {
					lastRefreshed = Long.parseLong(lastTimestampForTable);
				}

				if (lastRefreshed == -1 || (refreshInterval > 0 && currentTime >= (lastRefreshed + refreshInterval))) {
					stateMapProperties.remove(lastTimestampForTable);
				} else {
					refreshTable = false;
				}
			} catch (final NumberFormatException nfe) {
				getLogger().error(
						"Failed to retrieve observed last table fetches from the State Manager. Will not perform "
								+ "query until this is accomplished.",
						nfe);
				context.yield();
				return;
			}

			if (refreshTable) {
				logger.info("Found {}: {}", new Object[] { tableInformation.get(DB_TABLE_TYPE), fqn });

				if (includeCount) {
					try (Statement st = con.createStatement()) {
						final String countQuery = "SELECT COUNT(1) FROM " + fqn;

						logger.debug("Executing query: {}", new Object[] { countQuery });
						try (ResultSet countResult = st.executeQuery(countQuery)) {
							if (countResult.next()) {
								tableInformation.put(DB_TABLE_COUNT, Long.toString(countResult.getLong(1)));
							}
						}
					} catch (final SQLException se) {
						logger.error("Couldn't get row count for {}", new Object[] { fqn });
						continue;
					}
				}

				writer.addToListing(tableInformation, transitUri);
				stateMapProperties.put(fqn, Long.toString(System.currentTimeMillis()));
			}

		}
	}
    
    interface TableListingWriter {
        void beginListing() throws IOException, SchemaNotFoundException;

        void addToListing(Map<String, String> tableInformation, String transitUri) throws IOException;

        void finishListing() throws IOException;

        void finishListingExceptionally(Exception cause);
    }


    private static class AttributeTableListingWriter implements TableListingWriter {
        private final ProcessSession session;

        public AttributeTableListingWriter(final ProcessSession session) {
            this.session = session;
        }

        @Override
        public void beginListing() {
        }

        @Override
        public void addToListing(final Map<String, String> tableInformation, final String transitUri) {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, tableInformation);
            session.getProvenanceReporter().receive(flowFile, transitUri);
            session.transfer(flowFile, REL_SUCCESS);
        }

        @Override
        public void finishListing() {
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
        }
    }

    static class RecordTableListingWriter implements TableListingWriter {
        private static final RecordSchema RECORD_SCHEMA;
        public static final String TABLE_NAME = "tableName";
        public static final String TABLE_CATALOG = "catalog";
        public static final String TABLE_SCHEMA = "schemaName";
        public static final String TABLE_FULLNAME = "fullName";
        public static final String TABLE_TYPE = "tableType";
        public static final String TABLE_REMARKS = "remarks";
        public static final String TABLE_ROW_COUNT = "rowCount";


        static {
            final List<RecordField> fields = new ArrayList<>();
            fields.add(new RecordField(TABLE_NAME, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(TABLE_CATALOG, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(TABLE_SCHEMA, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(TABLE_FULLNAME, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(TABLE_TYPE, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(TABLE_REMARKS, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(TABLE_ROW_COUNT, RecordFieldType.LONG.getDataType(), false));
            RECORD_SCHEMA = new SimpleRecordSchema(fields);
        }


        private final ProcessSession session;
        private final RecordSetWriterFactory writerFactory;
        private final ComponentLog logger;
        private RecordSetWriter recordWriter;
        private FlowFile flowFile;
        private String transitUri;

        public RecordTableListingWriter(final ProcessSession session, final RecordSetWriterFactory writerFactory, final ComponentLog logger) {
            this.session = session;
            this.writerFactory = writerFactory;
            this.logger = logger;
        }

        @Override
        public void beginListing() throws IOException, SchemaNotFoundException {
            flowFile = session.create();

            final OutputStream out = session.write(flowFile);
            recordWriter = writerFactory.createWriter(logger, RECORD_SCHEMA, out, flowFile);
            recordWriter.beginRecordSet();
        }

        @Override
        public void addToListing(final Map<String, String> tableInfo, final String transitUri) throws IOException {
            this.transitUri = transitUri;
            recordWriter.write(createRecordForListing(tableInfo));
        }

        @Override
        public void finishListing() throws IOException {
            final WriteResult writeResult = recordWriter.finishRecordSet();
            recordWriter.close();

            if (writeResult.getRecordCount() == 0) {
                session.remove(flowFile);
            } else {
                final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                flowFile = session.putAllAttributes(flowFile, attributes);

                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().receive(flowFile, transitUri);
            }
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
            try {
                recordWriter.close();
            } catch (IOException e) {
                logger.error("Failed to write listing as Records due to {}", new Object[] {e}, e);
            }

            session.remove(flowFile);
        }

        private Record createRecordForListing(final Map<String, String> tableInfo) {
            final Map<String, Object> values = new HashMap<>();
            values.put(TABLE_NAME, tableInfo.get(DB_TABLE_NAME));
            values.put(TABLE_FULLNAME, tableInfo.get(DB_TABLE_FULLNAME));
            values.put(TABLE_CATALOG, tableInfo.get(DB_TABLE_CATALOG));
            values.put(TABLE_REMARKS, tableInfo.get(DB_TABLE_REMARKS));
            values.put(TABLE_SCHEMA, tableInfo.get(DB_TABLE_SCHEMA));
            values.put(TABLE_TYPE, tableInfo.get(DB_TABLE_TYPE));

            final String rowCountString = tableInfo.get(DB_TABLE_COUNT);
            if (rowCountString != null) {
                values.put(TABLE_ROW_COUNT, Long.parseLong(rowCountString));
            }

            return new MapRecord(RECORD_SCHEMA, values);
        }
    }

}
