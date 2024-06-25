package org.embulk.input.databricks.util;

import org.embulk.spi.Schema;
import org.junit.Assert;

public class TestingEmbulkUtil {
  public static void assertTypeEquals(Schema schema, String... types) {
    Assert.assertEquals(types.length, schema.getColumnCount());
    for (int i = 0; i < types.length; i++) {
      Assert.assertEquals(schema.getColumnType(i).getName(), types[i]);
    }
  }

  public static void assertNameEquals(Schema schema, String... names) {
    Assert.assertEquals(names.length, schema.getColumnCount());
    for (int i = 0; i < names.length; i++) {
      Assert.assertEquals(schema.getColumnName(i), names[i]);
    }
  }
}
