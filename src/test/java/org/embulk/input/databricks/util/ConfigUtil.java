package org.embulk.input.databricks.util;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.UUID;
import org.embulk.config.ConfigSource;
import org.embulk.input.DatabricksInputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.modules.ZoneIdModule;

public class ConfigUtil {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder()
          .addDefaultModules()
          .addModule(ZoneIdModule.withLegacyNames())
          .build();
  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  private static final String configEnvName = "EMBULK_INPUT_DATABRICKS_TEST_CONFIG";

  public static Boolean disableOnlineTest() {
    return isNullOrEmpty(System.getenv(configEnvName));
  }

  public static ConfigSource baseConfigSource() {
    return EmbulkTests.config(configEnvName);
  }

  public interface TestTask extends Task {
    @Config("server_hostname")
    public String getServerHostname();

    @Config("http_path")
    public String getHTTPPath();

    @Config("personal_access_token")
    public String getPersonalAccessToken();

    @Config("oauth2_client_id")
    public String getOauth2ClientId();

    @Config("oauth2_client_secret")
    public String getOauth2ClientSecret();

    @Config("catalog_name")
    public String getCatalogName();

    @Config("schema_name")
    public String getSchemaName();

    @Config("table_prefix")
    public String getTablePrefix();

    @Config("another_catalog_name")
    public String getAnotherCatalogName();
  }

  public static TestTask createTestTask() {
    return CONFIG_MAPPER.map(baseConfigSource(), TestTask.class);
  }

  public static String createAnotherCatalogQuotedFullTableName(String table) {
    final TestTask t = createTestTask();
    return String.format("`%s`.`%s`.`%s`", t.getAnotherCatalogName(), t.getSchemaName(), table);
  }

  public static String createQuotedFullTableName(String table) {
    final TestTask t = createTestTask();
    return String.format("`%s`.`%s`.`%s`", t.getCatalogName(), t.getSchemaName(), table);
  }

  public static String createRandomQuotedFullTableName() {
    return createQuotedFullTableName(createRandomTableName());
  }

  public static String createTableName(String tableSuffix) {
    final TestTask t = createTestTask();
    return t.getTablePrefix() + tableSuffix;
  }

  public static String createRandomTableName() {
    return createTableName(UUID.randomUUID().toString());
  }

  public static ConfigSource emptyConfigSource() {
    return CONFIG_MAPPER_FACTORY.newConfigSource();
  }

  public static ConfigSource createBasePluginConfigSource() {
    final TestTask t = createTestTask();

    return emptyConfigSource()
        .set("type", "databricks")
        .set("server_hostname", t.getServerHostname())
        .set("http_path", t.getHTTPPath())
        .set("personal_access_token", t.getPersonalAccessToken());
  }

  public static ConfigSource createPluginConfigSourceByQuery(String query) {
    return createBasePluginConfigSource().set("query", query);
  }

  public static ConfigSource createPluginConfigSourceByTable(String table) {
    final TestTask t = createTestTask();

    return createBasePluginConfigSource()
        .set("catalog_name", t.getCatalogName())
        .set("schema_name", t.getSchemaName())
        .set("table", table);
  }

  public static ConfigSource createPluginConfigSourceByAnotherCatalogTable(String table) {
    final TestTask t = createTestTask();

    return createBasePluginConfigSource()
        .set("catalog_name", t.getAnotherCatalogName())
        .set("schema_name", t.getSchemaName())
        .set("table", table);
  }

  public static DatabricksInputPlugin.DatabricksPluginTask createPluginTask(
      ConfigSource configSource) {
    return CONFIG_MAPPER.map(configSource, DatabricksInputPlugin.DatabricksPluginTask.class);
  }
}
