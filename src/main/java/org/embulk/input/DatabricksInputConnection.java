package org.embulk.input;

import java.lang.invoke.MethodHandles;
import java.sql.*;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksInputConnection extends JdbcInputConnection {
  private static final Logger logger =
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  final String catalogName;

  public DatabricksInputConnection(Connection connection, String catalogName, String schemaName)
      throws SQLException {
    super(new NoAutoCommitConnection(connection), schemaName);
    this.catalogName = catalogName;
    useCatalog(catalogName);
    useSchema(schemaName);
  }

  @Override
  protected void setSearchPath(String schema) throws SQLException {
    // There is nothing to do here as the schema needs to be configured after the catalogue has been
    // set up.
    // Also, the command to set the schema is unique to Databricks.
  }

  protected void useCatalog(String catalog) throws SQLException {
    // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-use-catalog.html
    if (catalog != null) {
      executeUpdate("USE CATALOG " + quoteIdentifierString(catalog));
    } else {
      String res = fetchOneColumn("SELECT CURRENT_CATALOG()");
      logger.debug("catalog_name is not set. current catalog is {}.", res);
    }
  }

  protected void useSchema(String schema) throws SQLException {
    // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-use-schema.html
    if (schema != null) {
      executeUpdate("USE SCHEMA " + quoteIdentifierString(schema));
    } else {
      String res = fetchOneColumn("SELECT CURRENT_SCHEMA()");
      logger.debug("schema_name is not set. current schema is {}.", res);
    }
  }

  protected String fetchOneColumn(String sql) throws SQLException {
    logger.info("SQL: " + sql);
    try (Statement stmt = connection.createStatement()) {
      ResultSet rs = stmt.executeQuery(sql);
      rs.next();
      return rs.getString(1);
    }
  }
}
