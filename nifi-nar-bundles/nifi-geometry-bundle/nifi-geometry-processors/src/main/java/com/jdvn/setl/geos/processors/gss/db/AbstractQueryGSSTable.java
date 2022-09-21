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
package com.jdvn.setl.geos.processors.gss.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
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

import com.cci.gss.jdbc.driver.IGSSConnection;
import com.cci.gss.jdbc.driver.IGSSResultSet;
import com.cci.gss.jdbc.driver.IGSSResultSetMetaData;
import com.cci.gss.jdbc.driver.IGSSStatement;
import com.jdvn.setl.geos.gss.GSSService;


public abstract class AbstractQueryGSSTable extends AbstractGSSFetchProcessor {

    public static final String RESULT_TABLENAME = "tablename";
    public static final String RESULT_ROW_COUNT = "querydbtable.row.count";
    public static final String GEO_COLUMN = "geo.column";
    public static final String GEO_FEATURE_TYPE = "geo.feature.type";
    public static final String GSS_FID = "FKEY";

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

	public static LayerMetadata getLayerMetadata(int layerId, Statement stmt) 
	throws SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT * FROM GSS.THEMES ");
		sb.append("WHERE THEME_ID=").append(layerId);
		
		return getLayerMetadata(sb, stmt);
	}
	public static LayerMetadata getLayerMetadata(String username, String name,
			Statement stmt) 
	throws SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT * FROM GSS.THEMES WHERE THEME_NAME='").append(name.toUpperCase());
		sb.append("' AND OWNER='").append(username.toUpperCase()).append("'");
		
		return getLayerMetadata(sb, stmt);
	}	
	public static LayerMetadata getLayerMetadata(StringBuffer querySb, Statement stmt) 
	throws SQLException {
		LayerMetadata md = null;
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(querySb.toString());
			if (!rs.next()) {
				return null;
			}
			
			md = new LayerMetadata();
			md.mThemeTableSchema = rs.getString("OWNER");
			md.mThemeTableName = rs.getString("THEME_NAME");
			md.mThemeId = rs.getInt("THEME_ID");
			md.mViewLink = rs.getInt("VLINK");
			md.mBitEncodeValue = rs.getInt("BIT_ENCODE_VALUE");
			
			if (md.mViewLink > 0) {
				rs.close();
				querySb.setLength(0);
				
				querySb.append("SELECT MINX, MINY, MAXX, MAXY, GRID_SIZE ");
				querySb.append("FROM GSS.THEMES ");
				querySb.append("WHERE THEME_ID=").append(md.mViewLink);
				rs = stmt.executeQuery(querySb.toString());
				if (!rs.next()) {
					return null;
				}
			}
			
			md.mMinX = rs.getDouble("MINX");
			md.mMinY = rs.getDouble("MINY");
			md.mMaxX = rs.getDouble("MAXX");
			md.mMaxY = rs.getDouble("MAXY");
			md.mGridSize = rs.getDouble("GRID_SIZE");
			
			rs.close();
			
			querySb.setLength(0);
			querySb.append("SELECT B.F_GEOMETRY_COLUMN, B.G_TABLE_SCHEMA, B.G_TABLE_NAME,");
			querySb.append(" B.GEOMETRY_TYPE, B.STORAGE_TYPE, C.SRID, C.SRTEXT ");
			querySb.append("FROM GSS.GEOMETRY_COLUMNS B, GSS.SPATIAL_REF_SYS C ");
			querySb.append("WHERE B.F_TABLE_NAME='").append(md.mThemeTableName).append("'");
			querySb.append(" AND B.SRID=C.SRID");
			
			rs = stmt.executeQuery(querySb.toString());
			if (!rs.next()) {
				return null;
			}
			
			md.mGeometryColumn = rs.getString(1);
			md.mGeometryTableSchema = rs.getString(2);
			md.mGeometryTableName = rs.getString(3);
			md.mGeometryType = rs.getInt(4);
			md.mStorageType = rs.getInt(5);
			md.mSrId = rs.getInt(6);
			md.mCrs = rs.getString(7);

			
			rs.close();
			
			return md;
		}
		finally {
			rs.close();
		}
	}	
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

    @OnScheduled
    public void setup(final ProcessContext context) {
        maxValueProperties = getDefaultMaxValueProperties(context, null);
    }

    @OnStopped
    public void stop() {
        // Reset the column type map in case properties change
        setupComplete.set(false);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        // Fetch the column/table info once
        if (!setupComplete.get()) {
            super.setup(context);
        }
        ProcessSession session = sessionFactory.createSession();
        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();

        final GSSService gssService = context.getProperty(GSS_SERVICE).asControllerService(GSSService.class);
        final DatabaseAdapter dbAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).evaluateAttributeExpressions().getValue();
        final String sqlQuery = context.getProperty(SQL_QUERY).evaluateAttributeExpressions().getValue();
        final String customWhereClause = context.getProperty(WHERE_CLAUSE).evaluateAttributeExpressions().getValue();
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

        final String selectQuery = getQuery(dbAdapter, tableName, sqlQuery, columnNames, null, customWhereClause, statePropertyMap);
        final StopWatch stopWatch = new StopWatch(true);
        final String fragmentIdentifier = UUID.randomUUID().toString();
        final IGSSConnection con = gssService.getConnection();
        try (
             final IGSSStatement st = con.createStatement()) {

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

            String jdbcURL = "GSSService";
            try {
                DatabaseMetaData databaseMetaData = con.getMetaData();
                if (databaseMetaData != null) {
                    jdbcURL = databaseMetaData.getURL();
                }
            } catch (SQLException se) {
                // Ignore and use default JDBC URL. This shouldn't happen unless the driver doesn't implement getMetaData() properly
            }

            //st.setQueryTimeout(queryTimeout); // timeout in seconds
            if (logger.isDebugEnabled()) {
                logger.debug("Executing query {}", new Object[] { selectQuery });
            }
            try (final IGSSResultSet resultSet = st.executeQuery(selectQuery)) {
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
                        attributesToAdd.put(RESULT_TABLENAME, tableName);
                        
                        IGSSResultSetMetaData rsmd = resultSet.getMetaData();
						if (rsmd.hasGeometryColumn()) {
							attributesToAdd.put(GeoAttributes.CRS.key(), rsmd.getWKTCoordinateReferenceSystem());
							attributesToAdd.put(GEO_COLUMN, rsmd.getGeometryColumn());
							double[] bbox = rsmd.getBounds();
							String envelop = "[[" + Double. toString(bbox[0]) + "," + Double. toString(bbox[2]) + "]" + ", [" + Double. toString(bbox[1]) + "," + Double. toString(bbox[3]) + "]]"; 
							attributesToAdd.put(GeoAttributes.GEO_ENVELOPE.key(), envelop);
							attributesToAdd.put(GeoAttributes.GEO_TYPE.key(), "Features");
							if (maxRowsPerFlowFile > 0) {
								attributesToAdd.put(GeoAttributes.GEO_NAME.key(),
										tableName + "_" + fragmentIdentifier + "_" + String.valueOf(fragmentIndex));
							} else
								attributesToAdd.put(GeoAttributes.GEO_NAME.key(), tableName);

							String featureType = "Point";
							switch (rsmd.getGeometryType()) {
								case 1:
									featureType = "Point";
									break;
								case 2:
									featureType = "LineString";
									break;
								case 3:
									featureType = "Polygon";
									break;
								case 4:
									featureType = "MultiPoint";
									break;
								case 5:
									featureType = "MultiLineString";
									break;
								case 6:
									featureType = "MultiPolygon";
									break;
								default:
									featureType = "Point";
							}
							attributesToAdd.put(GEO_FEATURE_TYPE, featureType);
							attributesToAdd.put(CoreAttributes.MIME_TYPE.key(), "application/avro+geowkt");
						}


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
            } catch (final Exception e) {
            	logger.error("Way, we have an error when execute SQL select query {} due to {}", new Object[]{selectQuery, e});
            } 
            session.transfer(resultSetFlowFiles, REL_SUCCESS);
            gssService.returnConnection(con);
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
            gssService.returnConnection(con);
            session.commitAsync();
        }
    }

    protected String getQuery(DatabaseAdapter dbAdapter, String tableName, String columnNames, List<String> maxValColumnNames,
                              String customWhereClause, Map<String, String> stateMap) {

        return getQuery(dbAdapter, tableName, null, columnNames, maxValColumnNames, customWhereClause, stateMap);
    }

    protected String getQuery(DatabaseAdapter dbAdapter, String tableName, String sqlQuery, String columnNames, List<String> maxValColumnNames,
                              String customWhereClause, Map<String, String> stateMap) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name must be specified");
        }
        final StringBuilder query;

        if (StringUtils.isEmpty(sqlQuery)) {
            query = new StringBuilder(dbAdapter.getSelectStatement(tableName, columnNames, null, null, null, null));
        } else {
            query = getWrappedQuery(dbAdapter, sqlQuery, tableName);
        }

        if (stateMap != null && !stateMap.isEmpty()) {
        	String maxValueKey = getStateKey(tableName, GSS_FID, dbAdapter);
        	String maxValue = stateMap.get(maxValueKey);
        	query.append(" WHERE FKEY > ").append(maxValue);
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
        	IGSSResultSet gssResultSet = (IGSSResultSet) resultSet;

			try {
				String fullyQualifiedMaxValueKey = getStateKey(tableName, GSS_FID, dbAdapter);
	        	String maxValueString = newColMap.get(fullyQualifiedMaxValueKey);
	            Integer maxIntValue = null;
	            if (maxValueString != null) {
	                maxIntValue = Integer.valueOf(maxValueString);
	            }

	        	Integer colIntValue = gssResultSet.getFID();
	            if (maxIntValue == null || colIntValue > maxIntValue) {
	            	newColMap.put(fullyQualifiedMaxValueKey, colIntValue.toString());
	            }    
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }

        @Override
        public void applyStateChanges() {
            this.originalState.putAll(this.newColMap);
        }
    }

    protected abstract SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context);
}
