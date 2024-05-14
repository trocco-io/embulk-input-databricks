package org.embulk.input.databricks;

import static org.embulk.test.EmbulkTests.readFile;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.input.databricks.util.ConfigUtil;
import org.embulk.input.databricks.util.ConnectionUtil;
import org.embulk.input.databricks.util.TestingEmbulkUtil;
import org.embulk.test.TestingEmbulk;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksInputPluginWithAbnormalType extends AbstractTestDatabricksInputPlugin {
  // embulk-input-jdbc does not support binary type.
  @Test
  public void testBinary() {
    ConfigSource configSource = ConfigUtil.createPluginConfigSourceByQuery("select X'1ABF'");
    PartialExecutionException e =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runInput(configSource, embulk.createTempFile("csv")));
    Assert.assertTrue(e.getCause() instanceof java.lang.UnsupportedOperationException);
  }

  @Test
  public void testBinaryWithTable() {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName = ConfigUtil.createQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (_c0 BINARY)", quotedFullTableName),
        String.format("INSERT INTO %s VALUES (X'1ABF')", quotedFullTableName));
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
            String.format("select _c0 from %s", quotedFullTableName));
    PartialExecutionException e =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runInput(configSource, embulk.createTempFile("csv")));
    Assert.assertTrue(e.getCause() instanceof java.lang.UnsupportedOperationException);
  }

  // Delta Lake does not support the INTERVAL type.
  // https://docs.databricks.com/en/sql/language-manual/data-types/interval-type.html
  @Test
  public void testInterval() throws IOException {
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
            "SELECT INTERVAL '100-00' YEAR TO MONTH as time");
    Path out = embulk.createTempFile("csv");
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals("100-0\n", readFile(out));
    TestingEmbulkUtil.assertNameEquals(runResult.getInputSchema(), "time");
    TestingEmbulkUtil.assertTypeEquals(runResult.getInputSchema(), "string");
  }

  @Test
  public void testIntervalWithTable() {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName = ConfigUtil.createQuotedFullTableName(tableName);
    RuntimeException e =
        Assert.assertThrows(
            RuntimeException.class,
            () ->
                ConnectionUtil.run(
                    String.format("create table %s (_c0 INTERVAL)", quotedFullTableName)));
    Assert.assertTrue(e.getCause() instanceof SQLException);
  }

  // Databricks JDBC does not support the TIMESTAMP_NTZ type.
  // https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html#notes
  @Test
  public void testTimestampNTZ() throws IOException {
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery("SELECT TIMESTAMP_NTZ'2020-12-31' as time");
    Path out = embulk.createTempFile("csv");
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals("2020-12-31 00:00:00.000000 +0000\n", readFile(out));
    TestingEmbulkUtil.assertNameEquals(runResult.getInputSchema(), "time");
    TestingEmbulkUtil.assertTypeEquals(runResult.getInputSchema(), "timestamp");
  }

  @Test
  public void testTimestampNTZWithTable() throws IOException {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName = ConfigUtil.createQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (_c0 TIMESTAMP_NTZ)", quotedFullTableName),
        String.format("INSERT INTO %s VALUES ( TIMESTAMP_NTZ'2020-12-31' )", quotedFullTableName));
    Path out = embulk.createTempFile("csv");
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
            String.format("select _c0 from %s", quotedFullTableName));
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals("2020-12-31 00:00:00.000000 +0000\n", readFile(out));
    TestingEmbulkUtil.assertNameEquals(runResult.getInputSchema(), "_c0");
    TestingEmbulkUtil.assertTypeEquals(runResult.getInputSchema(), "timestamp");
  }
}
