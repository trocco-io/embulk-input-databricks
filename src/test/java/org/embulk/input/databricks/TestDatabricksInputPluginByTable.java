package org.embulk.input.databricks;

import static org.embulk.input.databricks.util.TestingEmbulkUtil.assertNameEquals;
import static org.embulk.input.databricks.util.TestingEmbulkUtil.assertTypeEquals;
import static org.embulk.test.EmbulkTests.readFile;

import java.io.IOException;
import java.nio.file.Path;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.input.databricks.util.ConfigUtil;
import org.embulk.input.databricks.util.ConnectionUtil;
import org.embulk.test.TestingEmbulk;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksInputPluginByTable extends AbstractTestDatabricksInputPlugin {
  @Test
  public void testOnlyTable() throws IOException {
    runTest(
        null,
        null,
        null,
        "1,TEST0,2.0\n",
        new String[] {"_c0", "_c1", "_c2"},
        new String[] {"long", "string", "double"},
        "INSERT INTO %s VALUES (1, 'TEST0', 2.0)");
  }

  @Test
  public void testSelect() throws IOException {
    runTest(
        "_c2, _c0",
        null,
        "_c0",
        "2.0,1\n1.0,2\n3.0,3\n",
        new String[] {"_c2", "_c0"},
        new String[] {"double", "long"});
  }

  @Test
  public void testWhere() throws IOException {
    runTest(
        null,
        "_c1 like '%EST0'",
        "_c0",
        "1,TEST0,2.0\n2,TEST0,1.0\n",
        new String[] {"_c0", "_c1", "_c2"},
        new String[] {"long", "string", "double"});
  }

  @Test
  public void testOrderBy() throws IOException {
    runTest(
        null,
        null,
        "_c1 ASC, _c0 DESC",
        "2,TEST0,1.0\n1,TEST0,2.0\n3,TEST1,3.0\n",
        new String[] {"_c0", "_c1", "_c2"},
        new String[] {"long", "string", "double"});
  }

  @Test
  public void testAll() throws IOException {
    runTest("_c0", "_c2 > 1.9", "_c2 DESC", "3\n1\n", new String[] {"_c0"}, new String[] {"long"});
  }

  @Test
  public void testSameTableNameButOnlyDifferentCatalogName() throws IOException {
    String tableName = ConfigUtil.createRandomTableName();

    String quotedFullTableName0 = ConfigUtil.createQuotedFullTableName(tableName);
    String quotedFullTableName1 = ConfigUtil.createAnotherCatalogQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (a LONG, b DOUBLE)", quotedFullTableName0),
        String.format("insert into %s values (100, 10.0)", quotedFullTableName0),
        String.format("create table %s (x STRING)", quotedFullTableName1),
        String.format("insert into %s values ('TEST')", quotedFullTableName1));

    Path out0 = embulk.createTempFile("csv");
    TestingEmbulk.RunResult runResult0 =
        embulk.runInput(ConfigUtil.createPluginConfigSourceByTable(tableName), out0);
    Assert.assertEquals("100,10.0\n", readFile(out0));
    assertNameEquals(runResult0.getInputSchema(), "a", "b");
    assertTypeEquals(runResult0.getInputSchema(), "long", "double");

    Path out1 = embulk.createTempFile("csv");
    TestingEmbulk.RunResult runResult1 =
        embulk.runInput(ConfigUtil.createPluginConfigSourceByAnotherCatalogTable(tableName), out1);
    Assert.assertEquals("TEST\n", readFile(out1));
    assertNameEquals(runResult1.getInputSchema(), "x");
    assertTypeEquals(runResult1.getInputSchema(), "string");
  }

  @Test
  public void testRaiseErrorWhenCatalogNameDifferent() throws IOException {
    String tableName = ConfigUtil.createRandomTableName();

    String quotedFullTableName1 = ConfigUtil.createAnotherCatalogQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format("create table %s (x STRING)", quotedFullTableName1),
        String.format("insert into %s values ('TEST')", quotedFullTableName1));

    Assert.assertThrows(
        PartialExecutionException.class,
        () ->
            embulk.runInput(
                ConfigUtil.createPluginConfigSourceByTable(tableName),
                embulk.createTempFile("csv")));
  }

  private void runTest(
      String select,
      String where,
      String orderBy,
      String expectedCSV,
      String[] expectedColumnNames,
      String[] expectedTypeNames,
      String insertValue)
      throws IOException {
    String tableName = ConfigUtil.createRandomTableName();
    String quotedFullTableName = ConfigUtil.createQuotedFullTableName(tableName);
    ConnectionUtil.run(
        String.format(
            "create table %s (_c0 LONG PRIMARY KEY, _c1 STRING, _c2 DOUBLE)", quotedFullTableName),
        String.format(insertValue, quotedFullTableName));

    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByTable(tableName)
            .set("select", select)
            .set("where", where)
            .set("order_by", orderBy);
    Path out = embulk.createTempFile("csv");
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals(expectedCSV, readFile(out));
    assertNameEquals(runResult.getInputSchema(), expectedColumnNames);
    assertTypeEquals(runResult.getInputSchema(), expectedTypeNames);
  }

  private void runTest(
      String select,
      String where,
      String orderBy,
      String expectedCSV,
      String[] expectedColumnNames,
      String[] expectedTypeNames)
      throws IOException {
    runTest(
        select,
        where,
        orderBy,
        expectedCSV,
        expectedColumnNames,
        expectedTypeNames,
        "INSERT INTO %s VALUES (1, 'TEST0', 2.0), (2, 'TEST0', 1.0), (3, 'TEST1', 3.0)");
  }
}
