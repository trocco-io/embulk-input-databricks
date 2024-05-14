package org.embulk.input.databricks;

import static org.embulk.input.databricks.util.TestingEmbulkUtil.assertNameEquals;
import static org.embulk.input.databricks.util.TestingEmbulkUtil.assertTypeEquals;
import static org.embulk.test.EmbulkTests.readFile;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.input.databricks.util.ConfigUtil;
import org.embulk.input.databricks.util.ConnectionUtil;
import org.embulk.test.TestingEmbulk;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksInputPluginByQuery extends AbstractTestDatabricksInputPlugin {
  @Test
  public void testQuery() throws IOException {
    String quotedFullTableName = ConfigUtil.createRandomQuotedFullTableName();
    ConnectionUtil.run(
        String.format(
            "create table %s (_c0 LONG PRIMARY KEY, _c1 STRING, _c2 DOUBLE)", quotedFullTableName),
        String.format(
            "INSERT INTO %s VALUES (1, 'TEST0', 0.1), (2, 'TEST1', 0.2), (3, 'TEST1', 0.3)",
            quotedFullTableName));
    Path out = embulk.createTempFile("csv");
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
            String.format(
                "select _c2, _c0 from %s where _c1 like '%%EST1' order by _c0 DESC",
                quotedFullTableName));
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals("0.3,3\n0.2,2\n", readFile(out));
    assertNameEquals(runResult.getInputSchema(), "_c2", "_c0");
    assertTypeEquals(runResult.getInputSchema(), "double", "long");
  }

  @Test
  public void testQueryFoundByCatalogName() throws IOException {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName = ConfigUtil.createQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY)", quotedFullTableName),
        String.format("INSERT INTO %s VALUES (1)", quotedFullTableName));
    Path out = embulk.createTempFile("csv");
    ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
                String.format("select _c0 from `%s`.`%s`", t.getSchemaName(), tableName))
            .set("catalog_name", t.getCatalogName());
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals("1\n", readFile(out));
    assertNameEquals(runResult.getInputSchema(), "_c0");
    assertTypeEquals(runResult.getInputSchema(), "long");
  }

  @Test
  public void testQueryFoundByCatalogNameAndSchemaName() throws IOException {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName = ConfigUtil.createQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY)", quotedFullTableName),
        String.format("INSERT INTO %s VALUES (1)", quotedFullTableName));
    Path out = embulk.createTempFile("csv");
    ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(String.format("select _c0 from `%s`", tableName))
            .set("catalog_name", t.getCatalogName())
            .set("schema_name", t.getSchemaName());
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals("1\n", readFile(out));
    assertNameEquals(runResult.getInputSchema(), "_c0");
    assertTypeEquals(runResult.getInputSchema(), "long");
  }

  @Test
  public void testQueryNotFoundByNoCatalogNameAndSchemaName() throws IOException {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName = ConfigUtil.createQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY)", quotedFullTableName),
        String.format("INSERT INTO %s VALUES (1)", quotedFullTableName));
    Path out = embulk.createTempFile("csv");
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
            String.format("select _c0 from `%s`", tableName));
    PartialExecutionException e =
        Assert.assertThrows(
            PartialExecutionException.class, () -> embulk.runInput(configSource, out));
    Assert.assertTrue(e.getCause().getCause() instanceof SQLException);
  }
}
