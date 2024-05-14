package org.embulk.input.databricks;

import java.sql.*;
import org.embulk.input.databricks.util.ConfigUtil;
import org.embulk.input.databricks.util.ConnectionUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestConnectionUtil extends AbstractTestDatabricksInputPlugin {
  @Test
  public void TestConnect() {
    try (Connection con = ConnectionUtil.connectByTestTask()) {
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
          rs.next();
          Assert.assertEquals("1", rs.getString(1));
        }
      }
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void TestGetMetaData() {
    String quotedFullTableName = ConfigUtil.createRandomQuotedFullTableName();
    ConnectionUtil.run(
        String.format("create table %s (_c0 LONG PRIMARY KEY, _c1 STRING)", quotedFullTableName));
    try (Connection con = ConnectionUtil.connectByTestTask()) {
      assertMetaData(1, con, "select 1");
      assertMetaData(2, con, String.format("select * from %s", quotedFullTableName));
      assertMetaData(2, con, String.format("select * from %s where _c0 > 0", quotedFullTableName));

      // expected value is 2, but 0. (databricks jdbc bug?)
      // If that changes, enable use_raw_query_with_incremental support.
      // (see DatabricksInputPlugin.setupTask).
      assertMetaData(0, con, String.format("select * from %s where _c0 > ?", quotedFullTableName));
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static void assertMetaData(long expected, Connection conn, String query)
      throws SQLException {
    Assert.assertEquals(expected, conn.prepareStatement(query).getMetaData().getColumnCount());
  }
}
