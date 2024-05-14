package org.embulk.input.databricks;

import static org.embulk.input.databricks.util.TestingEmbulkUtil.assertNameEquals;
import static org.embulk.input.databricks.util.TestingEmbulkUtil.assertTypeEquals;
import static org.embulk.test.EmbulkTests.readFile;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.input.databricks.util.ConfigUtil;
import org.embulk.input.databricks.util.ConnectionUtil;
import org.embulk.test.TestingEmbulk;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestDatabricksInputPluginWithIncremental extends AbstractTestDatabricksInputPlugin {
  @Test
  public void testIncrementalColumns() throws IOException {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName = ConfigUtil.createQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY, _c1 STRING)", quotedFullTableName),
        String.format(
            "INSERT INTO %s VALUES (1,'TEST0'), (2, 'TEST0'), (2, 'TEST1'), (3, 'TEST2')",
            quotedFullTableName));
    Path out = embulk.createTempFile("csv");
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByTable(tableName)
            .set("incremental", true)
            .set("incremental_columns", new String[] {"_c0", "_c1"})
            .set("last_record", new String[] {"2", "TEST0"});
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals("2,TEST1\n3,TEST2\n", readFile(out));
    assertNameEquals(runResult.getInputSchema(), "_c0", "_c1");
    assertTypeEquals(runResult.getInputSchema(), "long", "string");
  }

  @Test
  public void testIncrementalColumnsNoLastRecord() throws IOException {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName = ConfigUtil.createQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY, _c1 STRING)", quotedFullTableName),
        String.format(
            "INSERT INTO %s VALUES (1,'TEST0'), (2, 'TEST0'), (2, 'TEST1'), (3, 'TEST2')",
            quotedFullTableName));
    Path out = embulk.createTempFile("csv");
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByTable(tableName)
            .set("incremental", true)
            .set("incremental_columns", new String[] {"_c0", "_c1"});
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals("1,TEST0\n2,TEST0\n2,TEST1\n3,TEST2\n", readFile(out));
    assertNameEquals(runResult.getInputSchema(), "_c0", "_c1");
    assertTypeEquals(runResult.getInputSchema(), "long", "string");
  }

  // If use_raw_query_with_incremental option is supported, remove @Ignore.
  @Test
  @Ignore
  public void testUseRawQueryWithIncremental() throws IOException {
    String quotedFullTableName = ConfigUtil.createRandomQuotedFullTableName();
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY, _c1 STRING)", quotedFullTableName),
        String.format(
            "INSERT INTO %s VALUES (1,'T0'), (2, 'T1'), (3, 'T2'), (4, 'T3')",
            quotedFullTableName));
    Path out = embulk.createTempFile("csv");

    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
                String.format(
                    "select %s._c0 as x from %s where %s._c0 > :x AND %s._c1 != 'T2'",
                    quotedFullTableName,
                    quotedFullTableName,
                    quotedFullTableName,
                    quotedFullTableName))
            .set("incremental", true)
            .set("incremental_columns", new String[] {"x"})
            .set("use_raw_query_with_incremental", true)
            .set("last_record", new String[] {"1"});
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals("2\n4\n", readFile(out));
    assertNameEquals(runResult.getInputSchema(), "x");
    assertTypeEquals(runResult.getInputSchema(), "long");
  }

  @Test
  public void testUseRawQueryWithIncrementalNotImplementation() throws IOException {
    String quotedFullTableName = ConfigUtil.createRandomQuotedFullTableName();
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY, _c1 STRING)", quotedFullTableName));
    Path out = embulk.createTempFile("csv");

    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
                String.format(
                    "select %s._c0 as x from %s where %s._c0 > :x AND %s._c1 != 'T2'",
                    quotedFullTableName,
                    quotedFullTableName,
                    quotedFullTableName,
                    quotedFullTableName))
            .set("incremental", true)
            .set("incremental_columns", new String[] {"x"})
            .set("use_raw_query_with_incremental", true)
            .set("last_record", new String[] {"1"});
    PartialExecutionException e =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runInput(configSource, embulk.createTempFile("csv")));
    assertTrue(e.getCause() instanceof ConfigException);
  }

  @Test
  public void testUseRawQueryWithIncrementalNoPlaceholderInQueryString() {
    String quotedFullTableName = ConfigUtil.createRandomQuotedFullTableName();
    ConnectionUtil.run(String.format("create table %s (_c0 LONG)", quotedFullTableName));
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
                String.format("select * from %s", quotedFullTableName))
            .set("incremental", true)
            .set("incremental_columns", new String[] {"_c0"})
            .set("use_raw_query_with_incremental", true);
    PartialExecutionException e =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runInput(configSource, embulk.createTempFile("csv")));
    assertTrue(e.getCause() instanceof ConfigException);
  }

  @Test
  public void testUseRawQueryWithIncrementalFalseWithQuery() {
    String quotedFullTableName = ConfigUtil.createRandomQuotedFullTableName();
    ConnectionUtil.run(String.format("create table %s (_c0 LONG)", quotedFullTableName));
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
                String.format("select * from %s", quotedFullTableName))
            .set("incremental", true);
    PartialExecutionException e =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runInput(configSource, embulk.createTempFile("csv")));
    assertTrue(e.getCause() instanceof ConfigException);
  }

  @Test
  public void testNoIncrementalColumns() throws IOException {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName = ConfigUtil.createQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY, _c1 STRING)", quotedFullTableName),
        String.format(
            "INSERT INTO %s VALUES (1,'TEST0'), (2, 'TEST1'), (3, 'TEST2')", quotedFullTableName));
    Path out = embulk.createTempFile("csv");
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByTable(tableName)
            .set("incremental", true)
            .set("last_record", new String[] {"2"});
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals("3,TEST2\n", readFile(out));
    assertNameEquals(runResult.getInputSchema(), "_c0", "_c1");
    assertTypeEquals(runResult.getInputSchema(), "long", "string");
  }

  @Test
  public void testNoIncrementalColumnsByDifferentSchemaButSameTableName() throws IOException {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName0 = ConfigUtil.createQuotedFullTableName(tableName);
    String quotedFullTableName1 = ConfigUtil.createAnotherCatalogQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY)", quotedFullTableName0),
        String.format("INSERT INTO %s VALUES (1), (2)", quotedFullTableName0));
    ConnectionUtil.run(
        String.format("create table %s (_c1 STRING PRIMARY KEY)", quotedFullTableName1),
        String.format("INSERT INTO %s VALUES ('T0'), ('T1')", quotedFullTableName1));

    Path out0 = embulk.createTempFile("csv");
    ConfigSource configSource0 =
        ConfigUtil.createPluginConfigSourceByTable(tableName)
            .set("incremental", true)
            .set("last_record", new String[] {"1"});
    TestingEmbulk.RunResult runResult0 = embulk.runInput(configSource0, out0);
    Assert.assertEquals("2\n", readFile(out0));
    assertNameEquals(runResult0.getInputSchema(), "_c0");
    assertTypeEquals(runResult0.getInputSchema(), "long");

    Path out1 = embulk.createTempFile("csv");
    ConfigSource configSource1 =
        ConfigUtil.createPluginConfigSourceByAnotherCatalogTable(tableName)
            .set("incremental", true)
            .set("last_record", new String[] {"T0"});
    TestingEmbulk.RunResult runResult1 = embulk.runInput(configSource1, out1);
    Assert.assertEquals("T1\n", readFile(out1));
    assertNameEquals(runResult1.getInputSchema(), "_c1");
    assertTypeEquals(runResult1.getInputSchema(), "string");
  }
}
