package org.embulk.input.databricks.util;

import java.sql.*;
import java.util.*;

public class ConnectionUtil {
  public static Connection connect(
      String serverHostname, String httpPath, String personalAccessToken)
      throws SQLException, ClassNotFoundException {
    Class.forName("com.databricks.client.jdbc.Driver");
    String url = String.format("jdbc:databricks://%s:443", serverHostname);
    Properties props = new java.util.Properties();
    props.put("httpPath", httpPath);
    props.put("AuthMech", "3");
    props.put("UID", "token");
    props.put("PWD", personalAccessToken);
    props.put("SSL", "1");
    return DriverManager.getConnection(url, props);
  }

  public static Connection connectByTestTask() throws SQLException, ClassNotFoundException {
    ConfigUtil.TestTask testTask = ConfigUtil.createTestTask();
    return connect(
        testTask.getServerHostname(), testTask.getHTTPPath(), testTask.getPersonalAccessToken());
  }

  public static void dropAllTemporaryTables() {
    ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    for (String catalogName : new String[] {t.getCatalogName(), t.getAnotherCatalogName()}) {
      String tableNamesSQL =
          String.format(
              "select table_name from system.information_schema.tables where table_catalog = '%s' AND table_schema = '%s' AND table_name LIKE '%s%%'",
              catalogName, t.getSchemaName(), t.getTablePrefix());
      runQuery(tableNamesSQL)
          .forEach(
              x -> {
                String tableName = (String) x.get("table_name");
                String dropSql =
                    String.format(
                        "drop table if exists `%s`.`%s`.`%s`",
                        catalogName, t.getSchemaName(), tableName);
                run(dropSql);
              });
    }
  }

  public static List<Map<String, Object>> runQuery(String query) {
    try (Connection conn = connectByTestTask();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      return toMap(rs);
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Map<String, Object>> toMap(ResultSet rs) throws SQLException {
    List<Map<String, Object>> result = new ArrayList<>();
    while (rs.next()) {
      Map<String, Object> resMap = new HashMap<>();
      for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
        resMap.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
      }
      result.add(resMap);
    }
    return result;
  }

  public static List<Boolean> run(String... queries) {
    try (Connection conn = connectByTestTask()) {
      List<Boolean> results = new ArrayList<>();
      for (String query : queries) {
        try (Statement stmt = conn.createStatement()) {
          results.add(stmt.execute(query));
        }
      }
      return results;
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
