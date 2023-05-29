
package com.jdvn.setl.geos.processors.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.GeoAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import com.jdvn.setl.geos.processors.util.GeoUtils;


public abstract class AbstractQueryPostGISTable extends AbstractPostGISFetchProcessor {

    public static final String RESULT_TABLENAME = "source.tablename";
    public static final String RESULT_ROW_COUNT = "querydbtable.row.count";
    public static final String RESULT_SCHEMANAME = "source.schemaname";
    public static final String RESULT_PKLIST = "source.pks";
    public static final String RESULT_URL = "source.url";
    public static final String STATEMENT_TYPE_ATTRIBUTE = "statement.type";

    private static AllowableValue TRANSACTION_READ_COMMITTED = new AllowableValue(
            String.valueOf(Connection.TRANSACTION_READ_COMMITTED),
            "TRANSACTION_READ_COMMITTED"
    );
    private static AllowableValue TRANSACTION_READ_UNCOMMITTED = new AllowableValue(
            String.valueOf(Connection.TRANSACTION_READ_UNCOMMITTED),
            "TRANSACTION_READ_UNCOMMITTED"
    );
    private static AllowableValue TRANSACTION_REPEATABLE_READ = new AllowableValue(
            String.valueOf(Connection.TRANSACTION_REPEATABLE_READ),
            "TRANSACTION_REPEATABLE_READ"
    );
    private static AllowableValue TRANSACTION_NONE =  new AllowableValue(
            String.valueOf(Connection.TRANSACTION_NONE),
            "TRANSACTION_NONE"
    );
    private static AllowableValue TRANSACTION_SERIALIZABLE = new AllowableValue(
            String.valueOf(Connection.TRANSACTION_SERIALIZABLE),
            "TRANSACTION_SERIALIZABLE"
    );

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch Size")
            .description("The number of result rows to be fetched from the result set at a time. This is a hint to the database driver and may not be "
                    + "honored and/or exact. If the value specified is zero, then the hint is ignored.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("qdbt-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("qdbt-output-batch-size")
            .displayName("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all result set rows "
                    + "have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles "
                    + "to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will "
                    + "be committed, thus releasing the FlowFiles to the downstream relationship. NOTE: The maxvalue.* and fragment.count attributes will not be set on FlowFiles when this "
                    + "property is set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_FRAGMENTS = new PropertyDescriptor.Builder()
            .name("qdbt-max-frags")
            .displayName("Maximum Number of Fragments")
            .description("The maximum number of fragments. If the value specified is zero, then all fragments are returned. " +
                    "This prevents OutOfMemoryError when this processor ingests huge table. NOTE: Setting this property can result in data loss, as the incoming results are "
                    + "not ordered, and fragments may end at arbitrary boundaries where rows are not included in the result set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor TRANS_ISOLATION_LEVEL = new PropertyDescriptor.Builder()
            .name("transaction-isolation-level")
            .displayName("Transaction Isolation Level")
            .description("This setting will set the transaction isolation level for the database connection for drivers that support this setting")
            .required(false)
            .allowableValues(TRANSACTION_NONE,TRANSACTION_READ_COMMITTED, TRANSACTION_READ_UNCOMMITTED, TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE)
            .build();

    public static final AllowableValue INITIAL_LOAD_STRATEGY_ALL_ROWS = new AllowableValue("Start at Beginning", "Start at Beginning", "Loads all existing rows from the database table.");
    public static final AllowableValue INITIAL_LOAD_STRATEGY_NEW_ROWS = new AllowableValue("Start at Current Maximum Values", "Start at Current Maximum Values", "Loads only the newly " +
            "inserted or updated rows based on the maximum value(s) of the column(s) configured in the '" + MAX_VALUE_COLUMN_NAMES.getDisplayName() + "' property.");

    public static final PropertyDescriptor INITIAL_LOAD_STRATEGY = new PropertyDescriptor.Builder()
            .name("initial-load-strategy")
            .displayName("Initial Load Strategy")
            .description("How to handle existing rows in the database table when the processor is started for the first time (or its state has been cleared). The property will be ignored, " +
                    "if any '" + INITIAL_MAX_VALUE_PROP_START + "*' dynamic property has also been configured.")
            .required(true)
            .allowableValues(INITIAL_LOAD_STRATEGY_ALL_ROWS, INITIAL_LOAD_STRATEGY_NEW_ROWS)
            .defaultValue(INITIAL_LOAD_STRATEGY_ALL_ROWS.getValue())
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final boolean maxValueColumnNames = validationContext.getProperty(MAX_VALUE_COLUMN_NAMES).isSet();
        final String initialLoadStrategy = validationContext.getProperty(INITIAL_LOAD_STRATEGY).getValue();
        if (!maxValueColumnNames && initialLoadStrategy.equals(INITIAL_LOAD_STRATEGY_NEW_ROWS.getValue())) {
            results.add(new ValidationResult.Builder().valid(false)
                    .subject(INITIAL_LOAD_STRATEGY.getDisplayName())
                    .input(INITIAL_LOAD_STRATEGY_NEW_ROWS.getDisplayName())
                    .explanation(String.format("'%s' strategy can only be used when '%s' property is also configured",
                            INITIAL_LOAD_STRATEGY_NEW_ROWS.getDisplayName(), MAX_VALUE_COLUMN_NAMES.getDisplayName()))
                    .build());
        }

        return results;
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        maxValueProperties = getDefaultMaxValueProperties(context, null);
    }

    @OnStopped
    public void stop() {
        setupComplete.set(false);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
		if (!setupComplete.get()) {
			super.setup(context);
		}
		getInserts(context, sessionFactory);
		getUpdates(context, sessionFactory);
		getDeletes(context, sessionFactory);
		
    }
    public void getInserts(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        // Fetch the column/table info once
        ProcessSession session = sessionFactory.createSession();
        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final DatabaseAdapter dbAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).evaluateAttributeExpressions().getValue();
        final String sqlQuery = context.getProperty(SQL_QUERY).evaluateAttributeExpressions().getValue();
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions().getValue();
        
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions().getValue();
        final String geoColumn = context.getProperty(GEO_COLUMN_NAME).evaluateAttributeExpressions().getValue();
        
        final String initialLoadStrategy = context.getProperty(INITIAL_LOAD_STRATEGY).getValue();
        final String customWhereClause = context.getProperty(WHERE_CLAUSE).evaluateAttributeExpressions().getValue();
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS).intValue();
        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer outputBatchSizeField = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        final Integer maxFragments = context.getProperty(MAX_FRAGMENTS).isSet()
                ? context.getProperty(MAX_FRAGMENTS).evaluateAttributeExpressions().asInteger()
                : 0;
        final Integer transIsolationLevel = context.getProperty(TRANS_ISOLATION_LEVEL).isSet()
                ? context.getProperty(TRANS_ISOLATION_LEVEL).asInteger()
                : null;

        SqlWriter sqlWriter = configureSqlWriter(session, context);

        final StateMap stateMap;
        try {
            stateMap = session.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve observed maximum values from the State Manager. Will not perform "
                    + "query until this is accomplished.", ioe);
            context.yield();
            return;
        }

        // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
        // set as the current state map (after the session has been committed)
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

        //If an initial max value for column(s) has been specified using properties, and this column is not in the state manager, sync them to the state property map
        for (final Map.Entry<String, String> maxProp : maxValueProperties.entrySet()) {
            String maxPropKey = maxProp.getKey().toLowerCase();
            String fullyQualifiedMaxPropKey = getStateKey(tableName, maxPropKey, dbAdapter);
            if (!statePropertyMap.containsKey(fullyQualifiedMaxPropKey)) {
                String newMaxPropValue;
                // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                // the value has been stored under a key that is only the column name. Fall back to check the column name,
                // but store the new initial max value under the fully-qualified key.
                if (statePropertyMap.containsKey(maxPropKey)) {
                    newMaxPropValue = statePropertyMap.get(maxPropKey);
                } else {
                    newMaxPropValue = maxProp.getValue();
                }
                statePropertyMap.put(fullyQualifiedMaxPropKey, newMaxPropValue);

            }
        }


        List<String> maxValueColumnNameList = StringUtils.isEmpty(maxValueColumnNames)
                ? null
                : Arrays.asList(maxValueColumnNames.split("\\s*,\\s*"));

        if (maxValueColumnNameList != null && statePropertyMap.isEmpty() && initialLoadStrategy.equals(INITIAL_LOAD_STRATEGY_NEW_ROWS.getValue())) {
            final String columnsClause = maxValueColumnNameList.stream()
                    .map(columnName -> String.format("MAX(%s) %s", columnName, columnName))
                    .collect(Collectors.joining(", "));

            final String selectMaxQuery = dbAdapter.getSelectStatement(tableName, columnsClause, null, null, null, null);

            try (final Connection con = dbcpService.getConnection(Collections.emptyMap());
                 final Statement st = con.createStatement()) {
	
                if (transIsolationLevel != null) {
                    con.setTransactionIsolation(transIsolationLevel);
                }

                st.setQueryTimeout(queryTimeout); // timeout in seconds

                try (final ResultSet resultSet = st.executeQuery(selectMaxQuery)) {
                    if (resultSet.next()) {
                        final MaxValueResultSetRowCollector maxValCollector = new MaxValueResultSetRowCollector(tableName, statePropertyMap, dbAdapter);
                        maxValCollector.processRow(resultSet);
                        maxValCollector.applyStateChanges();
                    }
                }
            } catch (final Exception e) {
                logger.error("Unable to execute SQL select query {} due to {}", new Object[]{selectMaxQuery, e});
                context.yield();
            }
        }

        List<String> columnNamesList = null;
        String selectQuery = null;
        
		try (final Connection con = dbcpService.getConnection(Collections.emptyMap());
				final Statement st = con.createStatement()) {
			
			columnNamesList = getAllTableColumnNames(con, schemaName, tableName);
			
			if (StringUtils.isEmpty(columnNames)) {
				selectQuery = getQuery(dbAdapter, tableName, sqlQuery, String.join(",", columnNamesList), geoColumn, maxValueColumnNameList, customWhereClause, statePropertyMap);
			} else {
				selectQuery = getQuery(dbAdapter, tableName, sqlQuery, columnNames, geoColumn, maxValueColumnNameList, customWhereClause, statePropertyMap);
			}

		} catch (final Exception e) {
			logger.error("Unable to execute SQL select query {}", new Object[] { e });
			context.yield();
		}      
		
        final StopWatch stopWatch = new StopWatch(true);
        final String fragmentIdentifier = UUID.randomUUID().toString();
        

        try (final Connection con = dbcpService.getConnection(Collections.emptyMap());
             final Statement st = con.createStatement()) {
        	
        	final List<String> Ids = getPrimaryKeyColumns(con, null, schemaName,tableName);
        	
            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException se) {
                    // Not all drivers support this, just log the error (at debug level) and move on
                    logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, se.getLocalizedMessage()}, se);
                }
            }

            if (transIsolationLevel != null) {
                con.setTransactionIsolation(transIsolationLevel);
            }

            String jdbcURL = "DBCPService";
            try {
                DatabaseMetaData databaseMetaData = con.getMetaData();
                if (databaseMetaData != null) {
                    jdbcURL = databaseMetaData.getURL();
                }
            } catch (SQLException se) {
                // Ignore and use default JDBC URL. This shouldn't happen unless the driver doesn't implement getMetaData() properly
            }

            st.setQueryTimeout(queryTimeout); // timeout in seconds
            if (logger.isDebugEnabled()) {
                logger.debug("Executing query {}", new Object[] { selectQuery });
            }
            
            final String crs = getCRSFromGeometryColumn(con, tableName, geoColumn);           
            
            try (final ResultSet resultSet = st.executeQuery(selectQuery)) {
                int fragmentIndex=0;
                // Max values will be updated in the state property map by the callback
                final MaxValueResultSetRowCollector maxValCollector = new MaxValueResultSetRowCollector(tableName, statePropertyMap, dbAdapter);

                while(true) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);

                    FlowFile fileToProcess = session.create();
                    try {
                        fileToProcess = session.write(fileToProcess, out -> {
                            try {
                                nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, getLogger(), maxValCollector));
                            } catch (Exception e) {
                                throw new ProcessException("Error during database query or conversion of records.", e);
                            }
                        });
                    } catch (ProcessException e) {
                        // Add flowfile to results before rethrowing so it will be removed from session in outer catch
                        resultSetFlowFiles.add(fileToProcess);
                        throw e;
                    }

                    if (nrOfRows.get() > 0) {
                        // set attributes
                        final Map<String, String> attributesToAdd = new HashMap<>();
                        attributesToAdd.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                        attributesToAdd.put(RESULT_SCHEMANAME, schemaName);
                        attributesToAdd.put(RESULT_TABLENAME, tableName);
                        attributesToAdd.put(RESULT_PKLIST, String.join(",", Ids));
                                                
						attributesToAdd.put(RESULT_URL, jdbcURL);
                        attributesToAdd.put(STATEMENT_TYPE_ATTRIBUTE, "INSERT");
                        
						attributesToAdd.put(GeoAttributes.CRS.key(), crs);
						attributesToAdd.put(GEO_COLUMN, geoColumn);
						attributesToAdd.put(GeoUtils.GEO_CHAR_SET, "UTF-8");
						

						double[] bbox = getExtentOfGeometryColumn(con, tableName, geoColumn);
						String envelop = "[[" + Double.toString(bbox[0]) + "," + Double.toString(bbox[2]) + "]"
								+ ", [" + Double.toString(bbox[1]) + "," + Double.toString(bbox[3]) + "]]";
						attributesToAdd.put(GeoAttributes.GEO_ENVELOPE.key(), envelop);
						attributesToAdd.put(GeoAttributes.GEO_TYPE.key(), "Features");
						
                        if(maxRowsPerFlowFile > 0) {
                            attributesToAdd.put(FRAGMENT_ID, fragmentIdentifier);
                            attributesToAdd.put(FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                        }
                        
						if (maxRowsPerFlowFile > 0) {
							attributesToAdd.put(GeoAttributes.GEO_NAME.key(),
									geoColumn + "_" + tableName + ":" + fragmentIdentifier + ":" + String.valueOf(fragmentIndex));
						} else
							attributesToAdd.put(GeoAttributes.GEO_NAME.key(), geoColumn + "_" + tableName);

                        attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
						if (attributesToAdd.get(GeoAttributes.CRS.key()) != null)
							attributesToAdd.put(CoreAttributes.MIME_TYPE.key(), "application/avro+geowkt");
						
                        fileToProcess = session.putAllAttributes(fileToProcess, attributesToAdd);
                        sqlWriter.updateCounters(session);

                        logger.debug("{} contains {} records; transferring to 'success'",
                                new Object[]{fileToProcess, nrOfRows.get()});

                        session.getProvenanceReporter().receive(fileToProcess, jdbcURL, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        resultSetFlowFiles.add(fileToProcess);
                        // If we've reached the batch size, send out the flow files
                        if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                            session.transfer(resultSetFlowFiles, REL_SUCCESS);
                            session.commitAsync();
                            resultSetFlowFiles.clear();
                        }
                    } else {
                        // If there were no rows returned, don't send the flowfile
                        session.remove(fileToProcess);
                        // If no rows and this was first FlowFile, yield
                        if(fragmentIndex == 0){
                            context.yield();
                        }
                        break;
                    }

                    fragmentIndex++;
                    if (maxFragments > 0 && fragmentIndex >= maxFragments) {
                        break;
                    }

                    // If we aren't splitting up the data into flow files or fragments, then the result set has been entirely fetched so don't loop back around
                    if (maxFragments == 0 && maxRowsPerFlowFile == 0) {
                        break;
                    }

                    // If we are splitting up the data into flow files, don't loop back around if we've gotten all results
                    if(maxRowsPerFlowFile > 0 && nrOfRows.get() < maxRowsPerFlowFile) {
                        break;
                    }
                }

                // Apply state changes from the Max Value tracker
                maxValCollector.applyStateChanges();

                // Even though the maximum value and total count are known at this point, to maintain consistent behavior if Output Batch Size is set, do not store the attributes
                if (outputBatchSize == 0) {
                    for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                        // Add maximum values as attributes
                        for (Map.Entry<String, String> entry : statePropertyMap.entrySet()) {
                            // Get just the column name from the key
                            String key = entry.getKey();
                            String colName = key.substring(key.lastIndexOf(NAMESPACE_DELIMITER) + NAMESPACE_DELIMITER.length());
                            resultSetFlowFiles.set(i, session.putAttribute(resultSetFlowFiles.get(i), "maxvalue." + colName, entry.getValue()));
                        }

                        //set count on all FlowFiles
                        if (maxRowsPerFlowFile > 0) {
                            resultSetFlowFiles.set(i,
                                    session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, Integer.toString(fragmentIndex)));
                        }
                    }
                }
            } catch (final SQLException e) {
                throw e;
            }

            session.transfer(resultSetFlowFiles, REL_SUCCESS);

        } catch (final ProcessException | SQLException e) {
            logger.error("Unable to execute SQL select query {} due to {}", new Object[]{selectQuery, e});
            if (!resultSetFlowFiles.isEmpty()) {
                session.remove(resultSetFlowFiles);
            }
            context.yield();
        } finally {
            try {
                // Update the state
                session.setState(statePropertyMap, Scope.CLUSTER);
            } catch (IOException ioe) {
                getLogger().error("{} failed to update State Manager, maximum observed values will not be recorded", new Object[]{this, ioe});
            }

            session.commitAsync();
        }
    }
   
	private double[] getExtentOfGeometryColumn(Connection connection, String layerName, String geoColumn){
		double[] bbox = {0,0,0,0};
		final StringBuilder query;
		query = new StringBuilder("SELECT ");
		query.append("min(ST_XMin(").append(geoColumn).append(")) as left,");
		query.append("min(ST_YMin(").append(geoColumn).append(")) as bottom,");
		query.append("max(ST_XMax(").append(geoColumn).append(")) as right,");
		query.append("max(ST_YMax(").append(geoColumn).append(")) as top");
		query.append(" FROM ").append(layerName);
		try (final Statement st = connection.createStatement()) {
	        try (final ResultSet resultSet = st.executeQuery(query.toString())) {
	            while (resultSet.next()) {
	            	bbox[0] = resultSet.getDouble("left");
	            	bbox[1] = resultSet.getDouble("bottom");
	            	bbox[2] = resultSet.getDouble("right");
	            	bbox[3] = resultSet.getDouble("top");
	            }
	        } catch (final SQLException e) {
	            throw e;
	        }	
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return bbox;		
	}    
	private String getCRSFromGeometryColumn(Connection connection, String layerName, String geoColumn){
		String crs_wkt = null;
		final StringBuilder query;
		query = new StringBuilder("SELECT srtext AS wkt FROM ");
		query.append("spatial_ref_sys");
		query.append(" WHERE srid = ");
		query.append("(SELECT Find_SRID('',").append("'").append(layerName).append("',").append("'").append(geoColumn).append("'))");
		
		try (final Statement st = connection.createStatement()) {
	        try (final ResultSet resultSet = st.executeQuery(query.toString())) {
	            while (resultSet.next()) {
	                crs_wkt = resultSet.getString("wkt");
	            }
	        } catch (final SQLException e) {
	            throw e;
	        }	
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return crs_wkt;		
	}
	private List<String> getAllTableColumnNames(Connection connection, String schemaName, String tableName){
		List<String> columns = new ArrayList<>();
		final StringBuilder query;
		query = new StringBuilder("SELECT column_name FROM information_schema.columns");
		query.append(" WHERE table_schema = ");
		query.append("'").append(schemaName).append("'");
		query.append(" AND table_name = ");
		query.append("'").append(tableName).append("'");
		
		try (final Statement st = connection.createStatement()) {
	        try (final ResultSet resultSet = st.executeQuery(query.toString())) {
	            while (resultSet.next()) {
	                columns.add(resultSet.getString("column_name"));
	            }
	        } catch (final SQLException e) {
	            throw e;
	        }	
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return columns;		
	}	
	private List<String> getPrimaryKeyColumns(Connection connection, String catalog, String schema, String layerName){
		List<String> columns = new ArrayList<>();
		try {
			DatabaseMetaData meta = connection.getMetaData();
	        try (ResultSet primaryKeys = meta.getPrimaryKeys(catalog, schema, layerName)) {
	            while (primaryKeys.next()) {
	                columns.add(primaryKeys.getString("COLUMN_NAME"));
	            }
	        }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return columns;		
	}
	
	protected String getQueryUpdate(Connection con, DatabaseAdapter dbAdapter, String tableName, Map<String, String> stateMap) {
		
		if (StringUtils.isEmpty(tableName)) {
			throw new IllegalArgumentException("Table name must be specified");
		}

		final StringBuilder query;
		String eventTable = getEventTableFromLayer(tableName);
		List<String> Ids = getPrimaryKeyColumns(con, null,null,tableName);
		query = new StringBuilder("SELECT S.*, to_char(E.Changed,'YYYY-MM-DD HH24.MI.SS.FF3') AS Changed FROM ");
		query.append(tableName).append(" S, ").append(eventTable).append(" E WHERE ");
		boolean first = true;
		for (String e : Ids) {
		    if (!first)
		    	query.append (" AND ");
		    else
		        first = false;
		    query.append ("S.").append(e).append(" = ").append("E.").append(e);
		}
		if (stateMap != null && !stateMap.isEmpty()) {
			String maxValueKey = getStateKey(eventTable, CDC_UPDATE_DATETIME, dbAdapter);
			String maxValue = stateMap.get(maxValueKey);
			if (maxValue != null)
				query.append(" AND to_char(E.Changed,'YYYY-MM-DD HH24.MI.SS.FF3') > ").append("'").append(maxValue).append("'");
		}
		return query.toString();

	}  
	protected String getQueryDelete(Connection con, DatabaseAdapter dbAdapter, String tableName, Map<String, String> stateMap) {
		
		if (StringUtils.isEmpty(tableName)) {
			throw new IllegalArgumentException("Table name must be specified");
		}
		final StringBuilder query;

		String eventTable = getEventTableFromLayer(tableName);
		List<String> Ids = getPrimaryKeyColumns(con, null,null,tableName);
		String IdsString = String.join(",", Ids);
		
		query = new StringBuilder("SELECT ");
		query.append(IdsString);
		query.append(", to_char(Changed,'YYYY-MM-DD HH24.MI.SS.FF3') AS Changed FROM ");
		query.append(eventTable);
		query.append(" WHERE Event='d'");
		
		if (stateMap != null && !stateMap.isEmpty()) {
			String maxValueKey = getStateKey(eventTable, CDC_DELETE_DATETIME, dbAdapter);
			String maxValue = stateMap.get(maxValueKey);
			if (maxValue != null)
				query.append(" AND to_char(Changed,'YYYY-MM-DD HH24.MI.SS.FF3') > ").append("'").append(maxValue).append("'");
		}
		return query.toString();

	} 	
    public void getUpdates(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();
        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final DatabaseAdapter dbAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions().getValue();
        final String geoColumn = context.getProperty(GEO_COLUMN_NAME).evaluateAttributeExpressions().getValue();
        
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS).intValue();
        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer outputBatchSizeField = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        final Integer maxFragments = context.getProperty(MAX_FRAGMENTS).isSet()
                ? context.getProperty(MAX_FRAGMENTS).evaluateAttributeExpressions().asInteger()
                : 0;
        final Integer transIsolationLevel = context.getProperty(TRANS_ISOLATION_LEVEL).isSet()
                ? context.getProperty(TRANS_ISOLATION_LEVEL).asInteger()
                : null;

        SqlWriter sqlWriter = configureSqlWriter(session, context);

        final StateMap stateMap;
        try {
            stateMap = session.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve observed maximum values from the State Manager. Will not perform "
                    + "query until this is accomplished.", ioe);
            context.yield();
            return;
        }

        // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
        // set as the current state map (after the session has been committed)
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

        final StopWatch stopWatch = new StopWatch(true);
        final String fragmentIdentifier = UUID.randomUUID().toString();
        String selectQuery = null;
         
        
        try (final Connection con = dbcpService.getConnection(Collections.emptyMap());
             final Statement st = con.createStatement()) {
        	
        	final List<String> Ids = getPrimaryKeyColumns(con, null, schemaName,tableName);
        	
        	selectQuery = getQueryUpdate(con, dbAdapter, tableName, statePropertyMap);

            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException se) {
                    // Not all drivers support this, just log the error (at debug level) and move on
                    logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, se.getLocalizedMessage()}, se);
                }
            }

            if (transIsolationLevel != null) {
                con.setTransactionIsolation(transIsolationLevel);
            }

            String jdbcURL = "DBCPService";
            try {
                DatabaseMetaData databaseMetaData = con.getMetaData();
                if (databaseMetaData != null) {
                    jdbcURL = databaseMetaData.getURL();
                }
            } catch (SQLException se) {
                // Ignore and use default JDBC URL. This shouldn't happen unless the driver doesn't implement getMetaData() properly
            }

            st.setQueryTimeout(queryTimeout); // timeout in seconds
            if (logger.isDebugEnabled()) {
                logger.debug("Executing query {}", new Object[] { selectQuery });
            }
            
            final String crs = getCRSFromGeometryColumn(con, tableName, geoColumn);
            
            try (final ResultSet resultSet = st.executeQuery(selectQuery)) {
                int fragmentIndex=0;
                // Max values will be updated in the state property map by the callback
                final MaxValueResultSetRowCollector maxValCollector = new MaxValueResultSetRowCollector(tableName, statePropertyMap, dbAdapter);

                while(true) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);

                    FlowFile fileToProcess = session.create();
                    try {
                        fileToProcess = session.write(fileToProcess, out -> {
                            try {
                                nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, getLogger(), maxValCollector));
                            } catch (Exception e) {
                                throw new ProcessException("Error during database query or conversion of records.", e);
                            }
                        });
                    } catch (ProcessException e) {
                        // Add flowfile to results before rethrowing so it will be removed from session in outer catch
                        resultSetFlowFiles.add(fileToProcess);
                        throw e;
                    }

                    if (nrOfRows.get() > 0) {
                        // set attributes
                        final Map<String, String> attributesToAdd = new HashMap<>();
                        attributesToAdd.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                        attributesToAdd.put(RESULT_SCHEMANAME, schemaName);
                        attributesToAdd.put(RESULT_TABLENAME, tableName);
                        attributesToAdd.put(RESULT_PKLIST, String.join(",", Ids));
						attributesToAdd.put(RESULT_URL, jdbcURL);                    
                        attributesToAdd.put(STATEMENT_TYPE_ATTRIBUTE, "UPDATE");
                        
						attributesToAdd.put(GeoAttributes.CRS.key(), crs);
						attributesToAdd.put(GEO_COLUMN, geoColumn);
						attributesToAdd.put(GeoAttributes.GEO_NAME.key(), geoColumn + "_" + tableName);
						attributesToAdd.put(GeoUtils.GEO_CHAR_SET, "UTF-8");
						
						double[] bbox = getExtentOfGeometryColumn(con, tableName, geoColumn);
						String envelop = "[[" + Double.toString(bbox[0]) + "," + Double.toString(bbox[2]) + "]"
								+ ", [" + Double.toString(bbox[1]) + "," + Double.toString(bbox[3]) + "]]";
						attributesToAdd.put(GeoAttributes.GEO_ENVELOPE.key(), envelop);
						attributesToAdd.put(GeoAttributes.GEO_TYPE.key(), "Features");
						
                        if(maxRowsPerFlowFile > 0) {
                            attributesToAdd.put(FRAGMENT_ID, fragmentIdentifier);
                            attributesToAdd.put(FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                        }
                        
						if (maxRowsPerFlowFile > 0) {
							attributesToAdd.put(GeoAttributes.GEO_NAME.key(),
									geoColumn + "_" + tableName + ":" + fragmentIdentifier + ":" + String.valueOf(fragmentIndex));
						} else
							attributesToAdd.put(GeoAttributes.GEO_NAME.key(), geoColumn + "_" + tableName);
						
                        attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
						if (attributesToAdd.get(GeoAttributes.CRS.key()) != null)
							attributesToAdd.put(CoreAttributes.MIME_TYPE.key(), "application/avro+geowkt");
						
                        fileToProcess = session.putAllAttributes(fileToProcess, attributesToAdd);
                        sqlWriter.updateCounters(session);

                        logger.debug("{} contains {} records; transferring to 'success'",
                                new Object[]{fileToProcess, nrOfRows.get()});

                        session.getProvenanceReporter().receive(fileToProcess, jdbcURL, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        resultSetFlowFiles.add(fileToProcess);
                        // If we've reached the batch size, send out the flow files
                        if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                            session.transfer(resultSetFlowFiles, REL_SUCCESS);
                            session.commitAsync();
                            resultSetFlowFiles.clear();
                        }
                    } else {
                        // If there were no rows returned, don't send the flowfile
                        session.remove(fileToProcess);
                        // If no rows and this was first FlowFile, yield
                        if(fragmentIndex == 0){
                            context.yield();
                        }
                        break;
                    }

                    fragmentIndex++;
                    if (maxFragments > 0 && fragmentIndex >= maxFragments) {
                        break;
                    }

                    // If we aren't splitting up the data into flow files or fragments, then the result set has been entirely fetched so don't loop back around
                    if (maxFragments == 0 && maxRowsPerFlowFile == 0) {
                        break;
                    }

                    // If we are splitting up the data into flow files, don't loop back around if we've gotten all results
                    if(maxRowsPerFlowFile > 0 && nrOfRows.get() < maxRowsPerFlowFile) {
                        break;
                    }
                }

                // Apply state changes from the Max Value tracker
                maxValCollector.applyStateChanges();

                // Even though the maximum value and total count are known at this point, to maintain consistent behavior if Output Batch Size is set, do not store the attributes
                if (outputBatchSize == 0) {
                    for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                        // Add maximum values as attributes
                        for (Map.Entry<String, String> entry : statePropertyMap.entrySet()) {
                            // Get just the column name from the key
                            String key = entry.getKey();
                            String colName = key.substring(key.lastIndexOf(NAMESPACE_DELIMITER) + NAMESPACE_DELIMITER.length());
                            resultSetFlowFiles.set(i, session.putAttribute(resultSetFlowFiles.get(i), "maxvalue." + colName, entry.getValue()));
                        }

                        //set count on all FlowFiles
                        if (maxRowsPerFlowFile > 0) {
                            resultSetFlowFiles.set(i,
                                    session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, Integer.toString(fragmentIndex)));
                        }
                    }
                }
            } catch (final SQLException e) {
                throw e;
            }

            session.transfer(resultSetFlowFiles, REL_SUCCESS);

        } catch (final ProcessException | SQLException e) {
            logger.error("Unable to execute SQL select query {} due to {}", new Object[]{selectQuery, e});
            if (!resultSetFlowFiles.isEmpty()) {
                session.remove(resultSetFlowFiles);
            }
            context.yield();
        } finally {
            try {
                // Update the state
                session.setState(statePropertyMap, Scope.CLUSTER);
            } catch (IOException ioe) {
                getLogger().error("{} failed to update State Manager, maximum observed values will not be recorded", new Object[]{this, ioe});
            }

            session.commitAsync();
        }
    }
     
    public void getDeletes(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();
        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final DatabaseAdapter dbAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions().getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS).intValue();
        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer outputBatchSizeField = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        final Integer maxFragments = context.getProperty(MAX_FRAGMENTS).isSet()
                ? context.getProperty(MAX_FRAGMENTS).evaluateAttributeExpressions().asInteger()
                : 0;
        final Integer transIsolationLevel = context.getProperty(TRANS_ISOLATION_LEVEL).isSet()
                ? context.getProperty(TRANS_ISOLATION_LEVEL).asInteger()
                : null;

        SqlWriter sqlWriter = configureSqlWriter(session, context);

        final StateMap stateMap;
        try {
            stateMap = session.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve observed maximum values from the State Manager. Will not perform "
                    + "query until this is accomplished.", ioe);
            context.yield();
            return;
        }

        // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
        // set as the current state map (after the session has been committed)
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

        String selectQuery = null;

        final StopWatch stopWatch = new StopWatch(true);
        final String fragmentIdentifier = UUID.randomUUID().toString();

        try (final Connection con = dbcpService.getConnection(Collections.emptyMap());
             final Statement st = con.createStatement()) {
        	
        	final List<String> Ids = getPrimaryKeyColumns(con, null, schemaName,tableName);
        	
        	selectQuery = getQueryDelete(con, dbAdapter, tableName, statePropertyMap);
        	

            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException se) {
                    // Not all drivers support this, just log the error (at debug level) and move on
                    logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, se.getLocalizedMessage()}, se);
                }
            }

            if (transIsolationLevel != null) {
                con.setTransactionIsolation(transIsolationLevel);
            }

            String jdbcURL = "DBCPService";
            try {
                DatabaseMetaData databaseMetaData = con.getMetaData();
                if (databaseMetaData != null) {
                    jdbcURL = databaseMetaData.getURL();
                }
            } catch (SQLException se) {
                // Ignore and use default JDBC URL. This shouldn't happen unless the driver doesn't implement getMetaData() properly
            }

            st.setQueryTimeout(queryTimeout); // timeout in seconds
            if (logger.isDebugEnabled()) {
                logger.debug("Executing query {}", new Object[] { selectQuery });
            }
            try (final ResultSet resultSet = st.executeQuery(selectQuery)) {
                int fragmentIndex=0;
                // Max values will be updated in the state property map by the callback
                final MaxValueResultSetRowCollector maxValCollector = new MaxValueResultSetRowCollector(tableName, statePropertyMap, dbAdapter);

                while(true) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);

                    FlowFile fileToProcess = session.create();
                    try {
                        fileToProcess = session.write(fileToProcess, out -> {
                            try {
                                nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, getLogger(), maxValCollector));
                            } catch (Exception e) {
                                throw new ProcessException("Error during database query or conversion of records.", e);
                            }
                        });
                    } catch (ProcessException e) {
                        // Add flowfile to results before rethrowing so it will be removed from session in outer catch
                        resultSetFlowFiles.add(fileToProcess);
                        throw e;
                    }

                    if (nrOfRows.get() > 0) {
                        // set attributes
                        final Map<String, String> attributesToAdd = new HashMap<>();
                        attributesToAdd.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                        attributesToAdd.put(RESULT_SCHEMANAME, schemaName);
                        attributesToAdd.put(RESULT_TABLENAME, tableName);
                        attributesToAdd.put(RESULT_PKLIST, String.join(",", Ids));
                        attributesToAdd.put(RESULT_URL, jdbcURL);   
                        attributesToAdd.put(STATEMENT_TYPE_ATTRIBUTE, "DELETE");

                        if(maxRowsPerFlowFile > 0) {
                            attributesToAdd.put(FRAGMENT_ID, fragmentIdentifier);
                            attributesToAdd.put(FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                        }

                        attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
                        fileToProcess = session.putAllAttributes(fileToProcess, attributesToAdd);
                        sqlWriter.updateCounters(session);

                        logger.debug("{} contains {} records; transferring to 'success'",
                                new Object[]{fileToProcess, nrOfRows.get()});

                        session.getProvenanceReporter().receive(fileToProcess, jdbcURL, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        resultSetFlowFiles.add(fileToProcess);
                        // If we've reached the batch size, send out the flow files
                        if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                            session.transfer(resultSetFlowFiles, REL_SUCCESS);
                            session.commitAsync();
                            resultSetFlowFiles.clear();
                        }
                    } else {
                        // If there were no rows returned, don't send the flowfile
                        session.remove(fileToProcess);
                        // If no rows and this was first FlowFile, yield
                        if(fragmentIndex == 0){
                            context.yield();
                        }
                        break;
                    }

                    fragmentIndex++;
                    if (maxFragments > 0 && fragmentIndex >= maxFragments) {
                        break;
                    }

                    // If we aren't splitting up the data into flow files or fragments, then the result set has been entirely fetched so don't loop back around
                    if (maxFragments == 0 && maxRowsPerFlowFile == 0) {
                        break;
                    }

                    // If we are splitting up the data into flow files, don't loop back around if we've gotten all results
                    if(maxRowsPerFlowFile > 0 && nrOfRows.get() < maxRowsPerFlowFile) {
                        break;
                    }
                }

                // Apply state changes from the Max Value tracker
                maxValCollector.applyStateChanges();

                // Even though the maximum value and total count are known at this point, to maintain consistent behavior if Output Batch Size is set, do not store the attributes
                if (outputBatchSize == 0) {
                    for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                        // Add maximum values as attributes
                        for (Map.Entry<String, String> entry : statePropertyMap.entrySet()) {
                            // Get just the column name from the key
                            String key = entry.getKey();
                            String colName = key.substring(key.lastIndexOf(NAMESPACE_DELIMITER) + NAMESPACE_DELIMITER.length());
                            resultSetFlowFiles.set(i, session.putAttribute(resultSetFlowFiles.get(i), "maxvalue." + colName, entry.getValue()));
                        }

                        //set count on all FlowFiles
                        if (maxRowsPerFlowFile > 0) {
                            resultSetFlowFiles.set(i,
                                    session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, Integer.toString(fragmentIndex)));
                        }
                    }
                }
            } catch (final SQLException e) {
                throw e;
            }

            session.transfer(resultSetFlowFiles, REL_SUCCESS);

        } catch (final ProcessException | SQLException e) {
            logger.error("Unable to execute SQL select query {} due to {}", new Object[]{selectQuery, e});
            if (!resultSetFlowFiles.isEmpty()) {
                session.remove(resultSetFlowFiles);
            }
            context.yield();
        } finally {
            try {
                // Update the state
                session.setState(statePropertyMap, Scope.CLUSTER);
            } catch (IOException ioe) {
                getLogger().error("{} failed to update State Manager, maximum observed values will not be recorded", new Object[]{this, ioe});
            }

            session.commitAsync();
        }
    }    
    
    protected String getQuery(DatabaseAdapter dbAdapter, String tableName, String columnNames, String geoColumn, List<String> maxValColumnNames,
                              String customWhereClause, Map<String, String> stateMap) {

        return getQuery(dbAdapter, tableName, null, columnNames, geoColumn, maxValColumnNames, customWhereClause, stateMap);
    }
    protected String getSelectStatement(String tableName, String columnNames, String geoColumnName) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        final StringBuilder query = new StringBuilder("SELECT ");
  
		boolean first = true;
        List<String> columnNamesList = new ArrayList<String>(Arrays.asList(columnNames.split(",")));
        for (String name: columnNamesList) {        	
		    if (!first)
		    	query.append (" , ");
		    else
		        first = false;
		    
        	if (name.toLowerCase().equals(geoColumnName.toLowerCase())) {
        		query.append("ST_AsText(").append(name).append(") as ").append(geoColumnName);
        	}else {
        		query.append(name);
        	}        	
        }
        query.append(" FROM ");
        query.append(tableName);
        return query.toString();
    }    
    
    protected String getQuery(DatabaseAdapter dbAdapter, String tableName, String sqlQuery, String columnNames, String geoColumn, List<String> maxValColumnNames,
                              String customWhereClause, Map<String, String> stateMap) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name must be specified");
        }
        final StringBuilder query;

        if (StringUtils.isEmpty(sqlQuery)) {
            //query = new StringBuilder(dbAdapter.getSelectStatement(tableName, columnNames, null, null, null, null));
            query = new StringBuilder(getSelectStatement(tableName, columnNames, geoColumn));
            
        } else {
            query = getWrappedQuery(dbAdapter, sqlQuery, tableName);
        }

        List<String> whereClauses = new ArrayList<>();
        // Check state map for last max values
        if (stateMap != null && !stateMap.isEmpty() && maxValColumnNames != null) {
            IntStream.range(0, maxValColumnNames.size()).forEach((index) -> {
                String colName = maxValColumnNames.get(index);
                String maxValueKey = getStateKey(tableName, colName, dbAdapter);
                String maxValue = stateMap.get(maxValueKey);
                if (StringUtils.isEmpty(maxValue)) {
                    // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                    // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                    // maximum value is observed, it will be stored under the fully-qualified key from then on.
                    maxValue = stateMap.get(colName.toLowerCase());
                }
                if (!StringUtils.isEmpty(maxValue)) {
                    Integer type = columnTypeMap.get(maxValueKey);
                    if (type == null) {
                        // This shouldn't happen as we are populating columnTypeMap when the processor is scheduled.
                        throw new IllegalArgumentException("No column type found for: " + colName);
                    }
                    // Add a condition for the WHERE clause
                    whereClauses.add(colName + (index == 0 ? " > " : " >= ") + getLiteralByType(type, maxValue, dbAdapter.getName()));
                }
            });
        }

        if (customWhereClause != null) {
            whereClauses.add("(" + customWhereClause + ")");
        }

        if (!whereClauses.isEmpty()) {
            query.append(" WHERE ");
            query.append(StringUtils.join(whereClauses, " AND "));
        }

        return query.toString();
    }

    public class MaxValueResultSetRowCollector implements JdbcCommon.ResultSetRowCallback {
        DatabaseAdapter dbAdapter;
        final Map<String, String> newColMap;
        final Map<String, String> originalState;
        String tableName;

        public MaxValueResultSetRowCollector(String tableName, Map<String, String> stateMap, DatabaseAdapter dbAdapter) {
            this.dbAdapter = dbAdapter;
            this.originalState = stateMap;

            this.newColMap = new HashMap<>();
            this.newColMap.putAll(stateMap);

            this.tableName = tableName;
        }

        @Override
        public void processRow(ResultSet resultSet) throws IOException {
            if (resultSet == null) {
                return;
            }
            try {
                // Iterate over the row, check-and-set max values
                final ResultSetMetaData meta = resultSet.getMetaData();
                final int nrOfColumns = meta.getColumnCount();
                String fullyQualifiedMaxValueKey;
                String maxValueString;
                 
                if (nrOfColumns > 0) {
                    for (int i = 1; i <= nrOfColumns; i++) {
                        String colName = meta.getColumnName(i).toLowerCase();
                        fullyQualifiedMaxValueKey = getStateKey(tableName, colName, dbAdapter);
                        Integer type = columnTypeMap.get(fullyQualifiedMaxValueKey);
                        // Skip any columns we're not keeping track of or whose value is null
                        if (type == null || resultSet.getObject(i) == null) {
                            continue;
                        }
                        maxValueString = newColMap.get(fullyQualifiedMaxValueKey);
                        // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                        // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                        // maximum value is observed, it will be stored under the fully-qualified key from then on.
                        if (StringUtils.isEmpty(maxValueString)) {
                            maxValueString = newColMap.get(colName);
                        }
                        String newMaxValueString = getMaxValueFromRow(resultSet, i, type, maxValueString, dbAdapter.getName());
                        if (newMaxValueString != null) {
                            newColMap.put(fullyQualifiedMaxValueKey, newMaxValueString);
                        }
                    }
                    
                    // Process event table for update & delete
                    for (int i = 1; i <= nrOfColumns; i++) {
                        String colName = meta.getColumnName(i).toLowerCase();
        	            if (colName.equals(CDC_EVENT_DATETIME.toLowerCase())) {
        	            	String setl_table = getEventTableFromLayer(tableName);
        	            	if (nrOfColumns > 3) {  // max column in event table = 3, to get update need more
        	            		fullyQualifiedMaxValueKey = getStateKey(setl_table, CDC_UPDATE_DATETIME, dbAdapter);
        			        	maxValueString = newColMap.get(fullyQualifiedMaxValueKey);	            		
        	            	}
        	            	else {
        	            		fullyQualifiedMaxValueKey = getStateKey(setl_table, CDC_DELETE_DATETIME, dbAdapter);
        			        	maxValueString = newColMap.get(fullyQualifiedMaxValueKey);	            		
        	            	}
        	            	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.S");
        	            	Date maxTimestampValue = null;
        		            if (maxValueString != null) {
        		            	maxTimestampValue = dateFormat.parse(maxValueString);
        		            }
        		        	String latestTime = resultSet.getString(CDC_EVENT_DATETIME);
        		        	Date colTimestampValue = dateFormat.parse(latestTime);
        		        	
        		            if (maxTimestampValue == null || colTimestampValue.compareTo(maxTimestampValue) > 0) {
        		            	newColMap.put(fullyQualifiedMaxValueKey, dateFormat.format(colTimestampValue));
        		            }  	            	
        	            }                         
                    }                  
                    
                }
                
                
            } catch (ParseException | SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void applyStateChanges() {
            this.originalState.putAll(this.newColMap);
        }
    }

    protected abstract SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context);
}
