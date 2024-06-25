package org.embulk.input.databricks;

import static org.embulk.input.databricks.util.TestingEmbulkUtil.assertTypeEquals;
import static org.embulk.test.EmbulkTests.readSortedFile;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.embulk.config.ConfigSource;
import org.embulk.input.databricks.util.ColumnOptionData;
import org.embulk.input.databricks.util.ConfigUtil;
import org.embulk.input.databricks.util.ConnectionUtil;
import org.embulk.input.databricks.util.DefaultColumnOptionData;
import org.embulk.test.TestingEmbulk;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksInputPluginWithType extends AbstractTestDatabricksInputPlugin {
  @Test
  public void testAllTypes() throws IOException {
    run(
        new TestSet("BIGINT", "200", "long", "200"),
        new TestSet("BOOLEAN", "true", "boolean", "true"),
        new TestSet("DATE", "'2020-03-04'", "timestamp", "2020-03-04 00:00:00.000000 +0000"),
        new TestSet("DECIMAL(4,2)", "12.25", "double", "12.25"),
        new TestSet("DOUBLE", "12.5", "double", "12.5"),
        new TestSet("FLOAT", "13.5", "double", "13.5"),
        new TestSet("INT", "300", "long", "300"),
        new TestSet("SMALLINT", "12", "long", "12"),
        new TestSet("STRING", "'test'", "string", "test"),
        new TestSet(
            "TIMESTAMP", "'2020-03-04 12:00:00Z'", "timestamp", "2020-03-04 12:00:00.000000 +0000"),
        new TestSet("TINYINT", "8", "long", "8"),
        new TestSet("ARRAY<INT>", "ARRAY(1, 2, 3)", "string", "\"[1,2,3]\""),
        new TestSet(
            "Map<STRING, INT>", "map('a',1,'b',2)", "string", "\"{\"\"a\"\":1,\"\"b\"\":2}\""),
        new TestSet(
            "STRUCT<c1:STRING, c2:INT>",
            "struct('test',2)",
            "string",
            "\"{\"\"c1\"\":\"\"test\"\",\"\"c2\"\":2}\""));
  }

  @Test
  public void testDefaultColumnOptions() throws IOException {
    run(
        c -> {
          DefaultColumnOptionData.create("BIGINT", "string", null).apply(c);
          DefaultColumnOptionData.create("DATE", "string", null).apply(c);
          DefaultColumnOptionData.create("VARCHAR", "json", null).apply(c);
          DefaultColumnOptionData.create("REAL", "string", null).apply(c);
          return c;
        },
        new TestSet("BIGINT", "200", "string", "200"),
        new TestSet("FLOAT", "12.5", "string", "12.5"),
        new TestSet("DATE", "'2020-03-04'", "string", "2020-03-04"),
        new TestSet("ARRAY<INT>", "ARRAY(1, 2, 3)", "json", "\"[1,2,3]\""),
        new TestSet(
            "Map<STRING, INT>", "map('a',1,'b',2)", "json", "\"{\"\"a\"\":1,\"\"b\"\":2}\""),
        new TestSet("STRUCT<c1:STRING>", "struct('test')", "json", "\"{\"\"c1\"\":\"\"test\"\"}\""),
        new TestSet("STRING", "'{\"a\":1,\"b\":2}'", "json", "\"{\"\"a\"\":1,\"\"b\"\":2}\""));
  }

  @Test
  public void testColumnOptions() throws IOException {
    run(
        c -> {
          ColumnOptionData.create("_c0", "json", null).apply(c);
          ColumnOptionData.create("_c1", "json", null).apply(c);
          ColumnOptionData.create("_c2", "json", null).apply(c);
          ColumnOptionData.create("_c3", "json", null).apply(c);
          ColumnOptionData.create("_c4", null, "%Y/%m/%d %H:%M:%S", null).apply(c);
          ColumnOptionData.create("_c5", null, "%Y-%m-%d %H:%M:%S", ZoneId.of("Asia/Tokyo"))
              .apply(c);
          ColumnOptionData.create("_c6", null, "%Y/%m/%d %H:%M:%S", null).apply(c);
          ColumnOptionData.create("_c7", null, "%Y-%m-%d %H:%M:%S", ZoneId.of("Asia/Tokyo"))
              .apply(c);
          ColumnOptionData.create("_c8", null, "long").apply(c);
          ColumnOptionData.create("_c9", null, "date").apply(c);
          ColumnOptionData.create("_c10", null, "string").apply(c);
          return c;
        },
        new TestSet("STRING", "'{\"a\":1,\"b\":2}'", "json", "\"{\"\"a\"\":1,\"\"b\"\":2}\""),
        new TestSet("ARRAY<INT>", "ARRAY(1, 2, 3)", "json", "\"[1,2,3]\""),
        new TestSet(
            "Map<STRING, INT>", "map('a',1,'b',2)", "json", "\"{\"\"a\"\":1,\"\"b\"\":2}\""),
        new TestSet("STRUCT<c1:STRING>", "struct('test')", "json", "\"{\"\"c1\"\":\"\"test\"\"}\""),
        new TestSet("TIMESTAMP", "'2020-03-04 12:00:00Z'", "string", "2020/03/04 12:00:00"),
        new TestSet("TIMESTAMP", "'2020-03-04 12:00:00Z'", "string", "2020-03-04 21:00:00"),
        new TestSet("DATE", "'2020-03-04'", "string", "2020/03/04 00:00:00"),
        new TestSet("DATE", "'2020-03-04'", "string", "2020-03-04 09:00:00"),
        new TestSet("STRING", "'2020'", "long", "2020"),
        new TestSet("STRING", "'2020-03-04'", "timestamp", "2020-03-04 00:00:00.000000 +0000"),
        new TestSet(
            "TIMESTAMP", "'2020-03-04 12:00:00Z'", "string", "2020-03-04 12:00:00.000000000"));
  }

  public class TestSet {
    final String tableType;
    final String tableValue;
    final String expectedValue;
    final String expectedType;

    public TestSet(String tableType, String tableValue, String expectedType, String expectedValue) {
      this.tableType = tableType;
      this.tableValue = tableValue;
      this.expectedValue = expectedValue;
      this.expectedType = expectedType;
    }
  }

  private void run(TestSet... testSets) throws IOException {
    run(null, testSets);
  }

  private void run(Function<ConfigSource, ConfigSource> convert, TestSet... testSets)
      throws IOException {
    String quotedFullTableName = ConfigUtil.createRandomQuotedFullTableName();
    ConnectionUtil.run(
        String.format(
            "create table %s (%s)",
            quotedFullTableName,
            IntStream.range(0, testSets.length)
                .mapToObj(i -> String.format("_c%d %s", i, testSets[i].tableType))
                .collect(Collectors.joining(" ,"))),
        String.format(
            "INSERT INTO %s VALUES (%s)",
            quotedFullTableName,
            Arrays.stream(testSets).map(x -> x.tableValue).collect(Collectors.joining(" ,"))));
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
            String.format("select * from %s", quotedFullTableName));
    if (convert != null) {
      configSource = convert.apply(configSource);
    }
    Path out = embulk.createTempFile("csv");
    TestingEmbulk.RunResult runResult = embulk.runInput(configSource, out);
    Assert.assertEquals(
        Arrays.stream(testSets).map(x -> x.expectedValue).collect(Collectors.joining(",")) + "\n",
        readSortedFile(out));
    assertTypeEquals(
        runResult.getInputSchema(),
        Arrays.stream(testSets).map(x -> x.expectedType).toArray(String[]::new));
  }
}
