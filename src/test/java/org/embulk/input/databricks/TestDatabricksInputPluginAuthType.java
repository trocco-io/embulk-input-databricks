package org.embulk.input.databricks;

import static org.embulk.test.EmbulkTests.readFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.input.databricks.util.ConfigUtil;
import org.embulk.input.databricks.util.ConnectionUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksInputPluginAuthType extends AbstractTestDatabricksInputPlugin {
  @Test
  public void testAuthTypeDefault() throws IOException {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    testRun((x) -> x.set("personal_access_token", t.getPersonalAccessToken()));
  }

  @Test
  public void testAuthTypePat() throws IOException {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    testRun(
        (x) -> x.set("auth_type", "pat").set("personal_access_token", t.getPersonalAccessToken()));
  }

  @Test
  public void testAtuTypePatWithoutPersonalAccessToken() {
    testConfigException((x) -> x.set("auth_type", "pat"), "personal_access_token");
  }

  @Test
  public void testAuthTypeM2MOauth() throws IOException {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    testRun(
        (x) ->
            x.set("auth_type", "oauth-m2m")
                .set("oauth2_client_id", t.getOauth2ClientId())
                .set("oauth2_client_secret", t.getOauth2ClientSecret()));
  }

  @Test
  public void testAuthTypeM2MOauthWithoutOauth2ClientId() {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    testConfigException(
        (x) ->
            x.set("auth_type", "oauth-m2m").set("oauth2_client_secret", t.getOauth2ClientSecret()),
        "oauth2_client_id");
  }

  @Test
  public void testAuthTypeM2MOauthWithoutOauth2ClientSecret() {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    testConfigException(
        (x) -> x.set("auth_type", "oauth-m2m").set("oauth2_client_id", t.getOauth2ClientId()),
        "oauth2_client_secret");
  }

  @Test
  public void testInvalidAuthType() {
    testConfigException((x) -> x.set("auth_type", "invalid"), "auth_type");
  }

  private void testRun(Function<ConfigSource, ConfigSource> setConfigSource) throws IOException {
    final String quotedFullTableName = ConfigUtil.createRandomQuotedFullTableName();
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG)", quotedFullTableName),
        String.format("INSERT INTO %s VALUES (1)", quotedFullTableName));
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    ConfigSource configSource =
        createMinimumConfigSource()
            .set("query", String.format("select * from %s", quotedFullTableName));
    Path out = embulk.createTempFile("csv");
    embulk.runInput(setConfigSource.apply(configSource), out);
    Assert.assertEquals("1\n", readFile(out));
  }

  private void testConfigException(
      Function<ConfigSource, ConfigSource> setConfigSource, String containedMessage) {
    ConfigSource configSource = createMinimumConfigSource();
    Path out = embulk.createTempFile("csv");
    Exception e =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runInput(setConfigSource.apply(configSource), out));
    Assert.assertTrue(e.getCause() instanceof ConfigException);
    Assert.assertTrue(
        String.format("「%s」 does not contains '%s'", e.getMessage(), containedMessage),
        e.getMessage().contains(containedMessage));
  }

  private ConfigSource createMinimumConfigSource() {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    return ConfigUtil.emptyConfigSource()
        .set("type", "databricks")
        .set("server_hostname", t.getServerHostname())
        .set("http_path", t.getHTTPPath());
  }
}
