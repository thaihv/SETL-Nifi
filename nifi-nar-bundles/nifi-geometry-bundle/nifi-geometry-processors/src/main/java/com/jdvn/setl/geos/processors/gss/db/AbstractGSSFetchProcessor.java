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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import com.jdvn.setl.geos.gss.GSSService;

/**
 * A base class for common code shared by processors that fetch RDBMS data.
 */
public abstract class AbstractGSSFetchProcessor extends AbstractSessionFactoryProcessor {

    public static final String INITIAL_MAX_VALUE_PROP_START = "initial.maxvalue.";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String GSS_FID = "FKEY";
    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();

    protected Set<Relationship> relationships;

    // Properties
    public static final PropertyDescriptor GSS_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the database.")
            .required(true)
            .identifiesControllerService(GSSService.class)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the database table to be queried.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Columns to Return")
            .description("A comma-separated list of column names to be used in the query. If your database requires "
                    + "special treatment of the names (quoting, e.g.), each name should include such treatment. If no "
                    + "column names are supplied, all columns in the specified table will be returned. NOTE: It is important "
                    + "to use consistent column names for a given table for incremental fetch to work properly.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor WHERE_CLAUSE = new PropertyDescriptor.Builder()
            .name("db-fetch-where-clause")
            .displayName("Additional WHERE clause")
            .description("A custom clause to be added in the WHERE condition when building SQL queries.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SQL_QUERY = new PropertyDescriptor.Builder()
            .name("db-fetch-sql-query")
            .displayName("Custom Query")
            .description("A custom SQL query used to retrieve data. Instead of building a SQL query from "
                    + "other properties, this query will be wrapped as a sub-query. Query must have no ORDER BY statement.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected List<PropertyDescriptor> propDescriptors;

    // The delimiter to use when referencing qualified names (such as table@!@column in the state map)
    protected static final String NAMESPACE_DELIMITER = "@!@";

    public static final PropertyDescriptor DB_TYPE;

    protected final static Map<String, DatabaseAdapter> dbAdapters = new HashMap<>();
    protected final Map<String, Integer> columnTypeMap = new HashMap<>();

    // This value is set when the processor is scheduled and indicates whether the Table Name property contains Expression Language.
    // It is used for backwards-compatibility purposes; if the value is false and the fully-qualified state key (table + column) is not found,
    // the processor will look for a state key with just the column name.
    protected volatile boolean isDynamicTableName = false;

    // This value is set when the processor is scheduled and indicates whether the Maximum Value Columns property contains Expression Language.
    // It is used for backwards-compatibility purposes; if the table name and max-value columns are static, then the column types can be
    // pre-fetched when the processor is scheduled, rather than having to populate them on-the-fly.
    protected volatile boolean isDynamicMaxValues = false;

    // This value is cleared when the processor is scheduled, and set to true after setup() is called and completes successfully. This enables
    // the setup logic to be performed in onTrigger() versus OnScheduled to avoid any issues with DB connection when first scheduled to run.
    protected final AtomicBoolean setupComplete = new AtomicBoolean(false);

    // A Map (name to value) of initial maximum-value properties, filled at schedule-time and used at trigger-time
    protected Map<String,String> maxValueProperties;

    static {
        // Load the DatabaseAdapters
        ArrayList<AllowableValue> dbAdapterValues = new ArrayList<>();
        ServiceLoader<DatabaseAdapter> dbAdapterLoader = ServiceLoader.load(DatabaseAdapter.class);
        dbAdapterLoader.forEach(it -> {
            dbAdapters.put(it.getName(), it);
            dbAdapterValues.add(new AllowableValue(it.getName(), it.getName(), it.getDescription()));
        });

        DB_TYPE = new PropertyDescriptor.Builder()
                .name("db-fetch-db-type")
                .displayName("Database Type")
                .description("The type/flavor of database, used for generating database-specific code. In many cases the Generic type "
                        + "should suffice, but some databases (such as Oracle) require custom SQL clauses. ")
                .allowableValues(dbAdapterValues.toArray(new AllowableValue[dbAdapterValues.size()]))
                .defaultValue("Generic")
                .required(true)
                .build();
    }

    // A common validation procedure for DB fetch processors, it stores whether the Table Name and/or Max Value Column properties have expression language
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        // For backwards-compatibility, keep track of whether the table name and max-value column properties are dynamic (i.e. has expression language)
        isDynamicTableName = validationContext.isExpressionLanguagePresent(validationContext.getProperty(TABLE_NAME).getValue());

        return super.customValidate(validationContext);
    }

    public void setup(final ProcessContext context) {
        setup(context,true,null);
    }

	boolean tableExists(Connection connection, String tableName) throws SQLException {
		DatabaseMetaData meta = connection.getMetaData();
		String schemaName = meta.getUserName();
		ResultSet resultSet = meta.getTables("%", schemaName, "%", new String[] { "TABLE" });
		while (resultSet.next()) {
			String currTableName = resultSet.getString("TABLE_NAME");
			if (tableName.toUpperCase().equals(currTableName.toUpperCase())) {
				return true;
			}
		}
		return false;
	}

	void createSETLEventTable(Connection connection, String tableName) throws SQLException {
		try {
			Statement stmt = connection.createStatement();
			final StringBuilder sqlBuilder = new StringBuilder();

			sqlBuilder.append("CREATE TABLE ");
			sqlBuilder.append(tableName);
			sqlBuilder.append("(FKEY NUMBER(9), ");
			sqlBuilder.append("Event VARCHAR2(16), ");
			sqlBuilder.append("Changed TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL)");

			stmt.execute(sqlBuilder.toString());
			System.out.println("Event Table " + tableName + " Created......");
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	void dropSETLEventTable(Connection connection, String tableName) throws SQLException {
		try {
			Statement stmt = connection.createStatement();
			final StringBuilder sqlBuilder = new StringBuilder();

			sqlBuilder.append("DROP TABLE ");
			sqlBuilder.append(tableName);

			stmt.execute(sqlBuilder.toString());
			System.out.println("Event Table " + tableName + " Dropped......");
			stmt.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	boolean triggerExists(Connection connection, String triggerName) throws SQLException {
		DatabaseMetaData meta = connection.getMetaData();
		String schemaName = meta.getUserName();
		ResultSet resultSet = meta.getTables("%", schemaName, "%", new String[] { "TRIGGER" });
		while (resultSet.next()) {
			String currTriggerName = resultSet.getString("TABLE_NAME");
			if (triggerName.toUpperCase().equals(currTriggerName.toUpperCase())) {
				return true;
			}
		}
		return false;
	}

	void createSETLTrigger(Connection connection, String triggerName, String layerName, String eventTableName) throws SQLException {
		try {
			Statement stmt = connection.createStatement();
			final StringBuilder sqlBuilder = new StringBuilder();

			sqlBuilder.append("CREATE OR REPLACE TRIGGER ");
			sqlBuilder.append(triggerName);
			sqlBuilder.append(" BEFORE UPDATE or DELETE ON ");
			sqlBuilder.append(layerName);
			sqlBuilder.append(" FOR EACH ROW ");
			sqlBuilder.append("DECLARE ");
			sqlBuilder.append("BEGIN ");
			sqlBuilder.append("IF UPDATING THEN ");
			sqlBuilder.append("INSERT INTO ");
			sqlBuilder.append(eventTableName);
			sqlBuilder.append(" VALUES(:old.SHAPE, 'u', SYSTIMESTAMP);");
			sqlBuilder.append("END IF;");
			sqlBuilder.append("IF DELETING THEN ");
			sqlBuilder.append("INSERT INTO ");
			sqlBuilder.append(eventTableName);
			sqlBuilder.append(" VALUES(:old.SHAPE, 'd', SYSTIMESTAMP);");			
			sqlBuilder.append("END IF;");
			sqlBuilder.append("END;");
			stmt.execute(sqlBuilder.toString());
			
			sqlBuilder.delete(0, sqlBuilder.length());
			sqlBuilder.append("ALTER TRIGGER ");
			sqlBuilder.append(triggerName);
			sqlBuilder.append(" ENABLE");
			stmt.execute(sqlBuilder.toString());
			System.out.println("SETL Trigger " + triggerName + " Created......");
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	void dropSETLTrigger(Connection connection, String triggerName) throws SQLException {
		try {
			Statement stmt = connection.createStatement();
			final StringBuilder sqlBuilder = new StringBuilder();

			sqlBuilder.append("DROP TRIGGER ");
			sqlBuilder.append(triggerName);
			
			stmt.execute(sqlBuilder.toString());
			System.out.println("SETL Trigger " + triggerName + " Dropped......");
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}  	
    public void setup(final ProcessContext context, boolean shouldCleanCache, FlowFile flowFile) {
		synchronized (setupComplete) {
			setupComplete.set(false);

			if (shouldCleanCache) {
				columnTypeMap.clear();
			}
			
			final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
			final DatabaseAdapter dbAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());
			String colKey = getStateKey(tableName, GSS_FID, dbAdapter);
			columnTypeMap.putIfAbsent(colKey, 4); // FID is a Integer Type 4

			// Create Tables and Trigers to catch events of DELETE and UPDATE on the same
			// GSS Store instance
			final GSSService gssService = context.getProperty(GSS_SERVICE).asControllerService(GSSService.class);
			final Connection con = gssService.getConnection();
			String setl_table = tableName + "_nifi_setl";
			String setl_trigger = tableName + "_nifi_setl_trackchanges";
			try {
				boolean bExist = tableExists(con, setl_table);
				if (!bExist) {
					createSETLEventTable(con, setl_table);
				} else {
					dropSETLEventTable(con, setl_table);
				}

				bExist = triggerExists(con, setl_trigger);
				if (!bExist) {
					createSETLTrigger(con, setl_trigger, tableName, setl_table);
				} else {
					dropSETLTrigger(con, setl_trigger);
				}
				
				//testFID(con);
				
				gssService.returnConnection(con);
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				gssService.returnConnection(con);
			}

			setupComplete.set(true);
			return;

		}
    }

    protected static StringBuilder getWrappedQuery(DatabaseAdapter dbAdapter, String sqlQuery, String tableName) {
       return new StringBuilder("SELECT * FROM (" + sqlQuery + ") " + dbAdapter.getTableAliasClause(tableName));
    }

    /**
     * Construct a key string for a corresponding state value.
     * @param prefix A prefix may contain database and table name, or just table name, this can be null
     * @param columnName A column name
     * @param adapter DatabaseAdapter is used to unwrap identifiers
     * @return a state key string
     */
    protected static String getStateKey(String prefix, String columnName, DatabaseAdapter adapter) {
        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
            sb.append(adapter.unwrapIdentifier(prefix.toLowerCase()));
            sb.append(NAMESPACE_DELIMITER);
        }
        if (columnName != null) {
            sb.append(adapter.unwrapIdentifier(columnName.toLowerCase()));
        }
        return sb.toString();
    }

    protected Map<String, String> getDefaultMaxValueProperties(final ProcessContext context, final FlowFile flowFile) {
        final Map<String, String> defaultMaxValues = new HashMap<>();

        context.getProperties().forEach((k, v) -> {
            final String key = k.getName();

            if (key.startsWith(INITIAL_MAX_VALUE_PROP_START)) {
                defaultMaxValues.put(key.substring(INITIAL_MAX_VALUE_PROP_START.length()), context.getProperty(k).evaluateAttributeExpressions(flowFile).getValue());
            }
        });
        return defaultMaxValues;
    }
}
