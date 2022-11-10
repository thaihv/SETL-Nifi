
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
import java.util.UUID;
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


public abstract class AbstractGSSFetchProcessor extends AbstractSessionFactoryProcessor {

    public static final String INITIAL_MAX_VALUE_PROP_START = "initial.maxvalue.";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String GSS_FID = "FKEY";
    public static final String GSS_UPDATE_DATETIME = "updated";
    public static final String GSS_DELETE_DATETIME = "deleted";
    public static final String GSS_EVENT_DATETIME = "Changed";
    public static final String GEO_COLUMN = "geo.column";
    public static final String GEO_FEATURE_TYPE = "geo.feature.type";    
    public static final String EVENT_PREFIX = "nifi_";
    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();

    protected Set<Relationship> relationships;

    // Properties
    public static final AllowableValue ON_USE = new AllowableValue("Use Exists", "Use Exists", "Keep using the tables and triggers created from previously SETL jobs");
    public static final AllowableValue RE_CREATED_ALL = new AllowableValue("Re-Created", "Re-Created", "Drop and create new all event tables and triggers to start SETL process");
    public static final AllowableValue RE_NEW_EVENTS = new AllowableValue("Renew-Table", "Renew-Table", "Erase all data from event tables");
    
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
    
    public static final PropertyDescriptor GENERATE_EVENT_TRACKERS = new PropertyDescriptor.Builder()
            .name("create-tables-and-triggers-for-setl")
            .displayName("Generate SETL event trackers")
            .description("Create tables and triggers to track changes from the source table. The information from this event trackers is useful to update to the target on PutGSS processor")
            .required(true)
            .allowableValues(ON_USE, RE_NEW_EVENTS, RE_CREATED_ALL)
            .defaultValue(ON_USE.getValue())
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
			getLogger().info("Event tracker table for " + tableName + " is created.!");
			stmt.close();					
		} catch (SQLException e) {
			getLogger().warn("Sorry, The table for event trackers can not created for some reason!");
			e.printStackTrace();
		}
	}
	void deleteAllFromSETLEventTable(Connection connection, String tableName) throws SQLException {
		try {
			Statement stmt = connection.createStatement();
			final StringBuilder sqlBuilder = new StringBuilder();

			sqlBuilder.append("DELETE FROM ");
			sqlBuilder.append(tableName);

			stmt.execute(sqlBuilder.toString());
			getLogger().info("All data in event tracker table for " + tableName + " is deleted.!");
			stmt.close();					
		} catch (SQLException e) {
			getLogger().warn("Sorry, The table for event trackers can not deleted for some reason!");
			e.printStackTrace();
		}
	}
	UUID createGUIDfromFkey(Connection connection, String tableName, int FID) {
		try {
			DatabaseMetaData meta = connection.getMetaData();
			String schemaName = meta.getUserName();
			String url = meta.getURL();
			String strEncode = url + "." + schemaName + "."+ tableName + "." + Integer.toString(FID);
			return UUID.nameUUIDFromBytes(strEncode.getBytes());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	void dropSETLEventTable(Connection connection, String tableName) throws SQLException {
		try {
			Statement stmt = connection.createStatement();
			final StringBuilder sqlBuilder = new StringBuilder();

			sqlBuilder.append("DROP TABLE ");
			sqlBuilder.append(tableName);

			stmt.execute(sqlBuilder.toString());
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

	String getGeometryTableNameFromGSS(Connection connection, String layerName) {
		try {
			Statement stmt = connection.createStatement();
			DatabaseMetaData meta = connection.getMetaData();
			String owner = meta.getUserName().toUpperCase();
			
			final StringBuilder sqlBuilder = new StringBuilder();

			sqlBuilder.append("SELECT THEME_ID FROM GSS.THEMES WHERE THEME_NAME = ");
			sqlBuilder.append("'").append(layerName).append("'");
			sqlBuilder.append(" AND ");
			sqlBuilder.append("OWNER = ");
			sqlBuilder.append("'").append(owner).append("'");
			
			ResultSet resultSet = stmt.executeQuery(sqlBuilder.toString()); 
			if (!resultSet.next()) {
				getLogger().warn("Table is not a layer, we can not get geometry info!");
				return null;
			}
			int n = resultSet.getInt("THEME_ID");
			resultSet.close();		
			stmt.close();
			return "G"+ Integer.toString(n);
			
		} catch (SQLException e) {
			getLogger().warn("Sorry, we can not get geometry info!");
			e.printStackTrace();
		}
		
		return null;
		
	}
	void createSETLTriggers(Connection connection, String layerName, String eventTableName) throws SQLException {
		
		try {
			Statement stmt = connection.createStatement();
			final StringBuilder sqlBuilder = new StringBuilder();
			
			// Create trigger for event of changes of DELETE and UPDATE attribute data
			String triggerName = eventTableName;
			sqlBuilder.delete(0, sqlBuilder.length());
			
			sqlBuilder.append("CREATE OR REPLACE TRIGGER ").append(triggerName);
			sqlBuilder.append(" BEFORE UPDATE or DELETE ON ").append(layerName);
			sqlBuilder.append(" FOR EACH ROW DECLARE FKEY NUMBER(9,0); ");
			sqlBuilder.append("BEGIN ");

			sqlBuilder.append("IF UPDATING THEN");
			sqlBuilder.append(" BEGIN");
			sqlBuilder.append(" SELECT DISTINCT FKEY into FKEY FROM ");
			sqlBuilder.append(eventTableName);
			sqlBuilder.append(" WHERE FKEY = :old.SHAPE; ");
			sqlBuilder.append(" EXCEPTION WHEN NO_DATA_FOUND THEN FKEY := NULL;");
			sqlBuilder.append(" END;");
			sqlBuilder.append("	IF FKEY is null THEN");
			sqlBuilder.append(" INSERT INTO ");
			sqlBuilder.append(eventTableName);
			sqlBuilder.append(" VALUES(:old.SHAPE, 'u', SYSTIMESTAMP);");
			sqlBuilder.append("	ELSE");
			sqlBuilder.append(" UPDATE ");
			sqlBuilder.append(eventTableName);
			sqlBuilder.append(" SET CHANGED = SYSTIMESTAMP WHERE FKEY = :old.SHAPE;");
			sqlBuilder.append("	END IF;"); // End Check FKEY
			sqlBuilder.append("END IF;"); // End UPDATING
			sqlBuilder.append("IF DELETING THEN");
			sqlBuilder.append(" INSERT INTO ");
			sqlBuilder.append(eventTableName);
			sqlBuilder.append(" VALUES(:old.SHAPE, 'd', SYSTIMESTAMP);");			
			sqlBuilder.append("END IF;"); // End DELETING
			
			sqlBuilder.append("END;");
			stmt.execute(sqlBuilder.toString());
			
			// Create trigger for event of changes UPDATE of geometry data
			String gtable = getGeometryTableNameFromGSS(connection, layerName);
			String gTrigger = EVENT_PREFIX + gtable;
			sqlBuilder.delete(0, sqlBuilder.length());
			sqlBuilder.append("CREATE OR REPLACE TRIGGER ").append(gTrigger);
			sqlBuilder.append(" AFTER UPDATE ON ").append(gtable);
			sqlBuilder.append(" FOR EACH ROW DECLARE FKEY NUMBER(9,0); ");
			sqlBuilder.append("BEGIN ");
			
			sqlBuilder.append("IF UPDATING THEN ");
			sqlBuilder.append(" BEGIN");
			sqlBuilder.append(" SELECT DISTINCT FKEY into FKEY FROM ");
			sqlBuilder.append(eventTableName);
			sqlBuilder.append(" WHERE FKEY = :old.GID; ");
			sqlBuilder.append(" EXCEPTION WHEN NO_DATA_FOUND THEN FKEY := NULL;");
			sqlBuilder.append(" END;");		
			
			sqlBuilder.append("	IF FKEY is null THEN");
			sqlBuilder.append(" INSERT INTO ");
			sqlBuilder.append(eventTableName);
			sqlBuilder.append(" VALUES(:old.GID, 'u', SYSTIMESTAMP);");
			sqlBuilder.append("	ELSE");
			sqlBuilder.append(" UPDATE ");
			sqlBuilder.append(eventTableName);
			sqlBuilder.append(" SET CHANGED = SYSTIMESTAMP WHERE FKEY = :old.GID;");
			sqlBuilder.append("	END IF;"); // End Check FKEY
			sqlBuilder.append("END IF;"); // End UPDATING
			
			sqlBuilder.append("END;");
			stmt.execute(sqlBuilder.toString());
			
			// Finally, enable all 02 triggers
			sqlBuilder.delete(0, sqlBuilder.length());
			sqlBuilder.append("ALTER TRIGGER ");
			sqlBuilder.append(triggerName);
			sqlBuilder.append(" ENABLE");
			stmt.execute(sqlBuilder.toString());
			
			sqlBuilder.delete(0, sqlBuilder.length());
			sqlBuilder.append("ALTER TRIGGER ");
			sqlBuilder.append(gTrigger);
			sqlBuilder.append(" ENABLE");
			stmt.execute(sqlBuilder.toString());
			
			getLogger().info("Triggers for event trackers are created.!");
			stmt.close();
		} catch (SQLException e) {
			getLogger().warn("Sorry, triggers for event trackers can not created for some reason!");
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
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public String getEventTableFromLayer(final String layerName) {
		String setl_table = EVENT_PREFIX + layerName;
		return setl_table.substring(0, Math.min(setl_table.length(), 30));		
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
			columnTypeMap.putIfAbsent(colKey, 4); // FID is a Integer Type 4 of java.sql.Types


			// Create Tables and Trigers to catch events of DELETE and UPDATE on the same
			// GSS Store instance
			final GSSService gssService = context.getProperty(GSS_SERVICE).asControllerService(GSSService.class);
			final String use_evt_trackers = context.getProperty(GENERATE_EVENT_TRACKERS).getValue();
			final Connection con = gssService.getConnection();
			
			String setl_table = getEventTableFromLayer(tableName);
			
			colKey = getStateKey(setl_table, GSS_UPDATE_DATETIME, dbAdapter);
			columnTypeMap.putIfAbsent(colKey, 93); // GSS_EVENT_DATETIME is a TIMESTAMP Type 93 of java.sql.Types		
			colKey = getStateKey(setl_table, GSS_DELETE_DATETIME, dbAdapter);
			columnTypeMap.putIfAbsent(colKey, 93); // GSS_EVENT_DATETIME is a TIMESTAMP Type 93 of java.sql.Types			

			try {
				boolean bExist = tableExists(con, setl_table);
				if (!bExist) {
					createSETLEventTable(con, setl_table);
				} else {
					if (use_evt_trackers.equals("Re-Created")) {
						dropSETLEventTable(con, setl_table);
						createSETLEventTable(con, setl_table);
					}else if (use_evt_trackers.equals("Renew-Table")) {
						deleteAllFromSETLEventTable(con, setl_table);
					}
						
					
				}

				bExist = triggerExists(con, setl_table);
				if (!bExist) {
					createSETLTriggers(con, tableName, setl_table);
				} else {
					if (use_evt_trackers.equals("Re-Created")) {
						dropSETLTrigger(con, setl_table);
						String gTrigger = EVENT_PREFIX + getGeometryTableNameFromGSS(con, tableName);
						dropSETLTrigger(con, gTrigger);
						createSETLTriggers(con, tableName, setl_table);
					}
				}
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
