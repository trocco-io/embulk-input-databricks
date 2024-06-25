package org.embulk.input.databricks;

import java.util.Properties;
import org.embulk.EmbulkSystemProperties;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.DatabricksInputPlugin;
import org.embulk.input.databricks.util.ConfigUtil;
import org.embulk.input.databricks.util.ConnectionUtil;
import org.embulk.output.file.LocalFileOutputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

public class AbstractTestDatabricksInputPlugin {
  private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES =
      EmbulkSystemProperties.of(new Properties());

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
          .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
          .registerPlugin(FileOutputPlugin.class, "file", LocalFileOutputPlugin.class)
          .registerPlugin(InputPlugin.class, "databricks", DatabricksInputPlugin.class)
          .build();

  @Before
  public void setup() {
    if (ConfigUtil.disableOnlineTest()) {
      return;
    }
    ConnectionUtil.dropAllTemporaryTables();
  }

  @After
  public void cleanup() {
    if (ConfigUtil.disableOnlineTest()) {
      return;
    }
    ConnectionUtil.dropAllTemporaryTables();
  }
}
