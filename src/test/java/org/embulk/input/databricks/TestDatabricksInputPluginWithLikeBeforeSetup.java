package org.embulk.input.databricks;

import static org.embulk.input.databricks.util.TestingEmbulkUtil.assertNameEquals;
import static org.embulk.input.databricks.util.TestingEmbulkUtil.assertTypeEquals;
import static org.embulk.test.EmbulkTests.readFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.embulk.config.ConfigSource;
import org.embulk.input.databricks.util.ConfigUtil;
import org.embulk.input.databricks.util.ConnectionUtil;
import org.embulk.test.TestingEmbulk;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDatabricksInputPluginWithLikeBeforeSetup
    extends AbstractTestDatabricksInputPlugin {
  String quotedFullTableName;
  Path out;
  ConfigSource configSource;

  @Before
  public void setup() {
    super.setup();
    quotedFullTableName = ConfigUtil.createRandomQuotedFullTableName();
    out = embulk.createTempFile("csv");
    configSource =
        ConfigUtil.createPluginConfigSourceByQuery(
            String.format("select * from %s", quotedFullTableName));
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY)", quotedFullTableName),
        String.format("INSERT INTO %s VALUES (1)", quotedFullTableName));
  }

  @Test
  public void testBeforeSetup() throws IOException {
    configSource =
        configSource.set(
            "before_setup", String.format("update %s set _c0 = 3", quotedFullTableName));

    TestingEmbulk.RunResult embulkRunResult = embulk.runInput(configSource, out);
    Assert.assertEquals("3\n", readFile(out));
    assertNameEquals(embulkRunResult.getInputSchema(), "_c0");
    assertTypeEquals(embulkRunResult.getInputSchema(), "long");
  }

  @Test
  public void testBeforeSelect() throws IOException {
    configSource =
        configSource.set(
            "before_select", String.format("update %s set _c0 = 3", quotedFullTableName));

    TestingEmbulk.RunResult embulkRunResult = embulk.runInput(configSource, out);
    Assert.assertEquals("3\n", readFile(out));
    assertNameEquals(embulkRunResult.getInputSchema(), "_c0");
    assertTypeEquals(embulkRunResult.getInputSchema(), "long");
  }

  @Test
  public void testAfterSelect() throws IOException {
    configSource =
        configSource.set(
            "after_select", String.format("update %s set _c0 = 3", quotedFullTableName));

    TestingEmbulk.RunResult embulkRunResult = embulk.runInput(configSource, out);
    Assert.assertEquals("1\n", readFile(out));
    assertNameEquals(embulkRunResult.getInputSchema(), "_c0");
    assertTypeEquals(embulkRunResult.getInputSchema(), "long");

    List<Map<String, Object>> afterResult =
        ConnectionUtil.runQuery(String.format("select * from %s", quotedFullTableName));
    Assert.assertEquals(afterResult.get(0).get("_c0"), 3L);
  }
}
