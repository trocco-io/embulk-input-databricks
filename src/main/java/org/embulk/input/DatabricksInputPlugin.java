package org.embulk.input;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import org.embulk.config.ConfigException;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksInputPlugin extends AbstractJdbcInputPlugin {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksInputPlugin.class);

  public interface DatabricksPluginTask extends PluginTask {
    @Config("driver_path")
    @ConfigDefault("null")
    public Optional<String> getDriverPath();

    @Config("server_hostname")
    public String getServerHostname();

    @Config("http_path")
    public String getHTTPPath();

    @Config("personal_access_token")
    public String getPersonalAccessToken();

    @Config("catalog_name")
    @ConfigDefault("null")
    public Optional<String> getCatalogName();

    @Config("schema_name")
    @ConfigDefault("null")
    public Optional<String> getSchemaName();

    @Config("user_agent")
    @ConfigDefault("{}")
    public UserAgentEntry getUserAgentEntry();

    public interface UserAgentEntry extends Task {
      @Config("product_name")
      @ConfigDefault("\"unknown\"")
      public String getProductName();

      @Config("product_version")
      @ConfigDefault("\"0.0.0\"")
      public String getProductVersion();
    }
  }

  @Override
  protected Class<? extends PluginTask> getTaskClass() {
    return DatabricksPluginTask.class;
  }

  @Override
  protected JdbcInputConnection newConnection(PluginTask task) throws SQLException {
    // https://docs.databricks.com/en/integrations/jdbc/index.html
    // https://docs.databricks.com/en/integrations/jdbc/authentication.html
    // https://docs.databricks.com/en/integrations/jdbc/compute.html
    DatabricksPluginTask t = (DatabricksPluginTask) task;
    if (t.getDriverPath().isPresent()) {
      addDriverJarToClasspath(t.getDriverPath().get());
    } else {
      try {
        Class.forName("com.databricks.client.jdbc.Driver");
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    String url = String.format("jdbc:databricks://%s:443", t.getServerHostname());
    Properties props = new java.util.Properties();
    props.put("httpPath", t.getHTTPPath());
    props.put("AuthMech", "3");
    props.put("UID", "token");
    props.put("PWD", t.getPersonalAccessToken());
    props.put("SSL", "1");
    if (t.getCatalogName().isPresent()) {
      props.put("ConnCatalog", t.getCatalogName().get());
    }
    if (t.getSchemaName().isPresent()) {
      props.put("ConnSchema", t.getSchemaName().get());
    }
    props.putAll(t.getOptions());

    // overwrite UserAgentEntry property if the same property is set in options
    String productName = t.getUserAgentEntry().getProductName();
    String productVersion = t.getUserAgentEntry().getProductVersion();
    props.put("UserAgentEntry", productName + "/" + productVersion);

    logConnectionProperties(url, props);
    Connection c = DriverManager.getConnection(url, props);
    return new DatabricksInputConnection(
        c, t.getCatalogName().orElse(null), t.getSchemaName().orElse(null));
  }

  @Override
  protected void logConnectionProperties(String url, Properties props) {
    Properties maskedProps = new Properties();
    for (Object keyObj : props.keySet()) {
      String key = (String) keyObj;
      String maskedVal = key.equals("PWD") ? "***" : props.getProperty(key);
      maskedProps.setProperty(key, maskedVal);
    }
    super.logConnectionProperties(url, maskedProps);
  }

  @Override
  protected Schema setupTask(JdbcInputConnection con, PluginTask task) throws SQLException {
    if (task.getUseRawQueryWithIncremental()) {
      // spotless:off
      // A query metadata with placeholder result is empty in databricks jdbc.
      //
      // Example:
      //   CREATE TABLE t (_c0 LONG PRIMARY KEY, _c1 STRING)
      //   connection().prepareStatement("select * from t where c0 > ?").getMetaData().getColumnCount(); // 0
      //
      // So, the input schema cannot be determined by the query, use_raw_query_with_incremental is not supported.
      // If that behaviour changes, enable use_raw_query_with_incremental support.
      // spotless:on
      throw new ConfigException("use_raw_query_with_incremental option is not supported.");
    }
    return super.setupTask(con, task);
  }
}
