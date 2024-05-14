package org.embulk.input.databricks.util;

import java.time.ZoneId;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.modules.ZoneIdModule;

public class ColumnOptionData {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder()
          .addDefaultModules()
          .addModule(ZoneIdModule.withLegacyNames())
          .build();

  final String columnName;
  final String type;
  final String valueType;
  final String timestampFormat;
  final ZoneId timeZone;

  private ColumnOptionData(
      String columnName, String type, String valueType, String timestampFormat, ZoneId timeZone) {
    this.columnName = columnName;
    this.type = type;
    this.valueType = valueType;
    this.timestampFormat = timestampFormat;
    this.timeZone = timeZone;
  }

  public static ColumnOptionData create(String columnName, String type, String valueType) {
    return new ColumnOptionData(columnName, type, valueType, null, null);
  }

  public static ColumnOptionData create(
      String columnName, String valueType, String timestampFormat, ZoneId timeZone) {
    return new ColumnOptionData(columnName, "string", valueType, timestampFormat, timeZone);
  }

  public ConfigSource apply(ConfigSource configSource) {
    ConfigSource columnOption = CONFIG_MAPPER_FACTORY.newConfigSource();
    if (type != null) {
      columnOption.set("type", type);
    }
    if (valueType != null) {
      columnOption.set("value_type", valueType);
    }
    if (timestampFormat != null) {
      columnOption.set("timestamp_format", timestampFormat);
    }
    if (timeZone != null) {
      columnOption.set("timezone", timeZone);
    }
    ConfigSource columnOptions =
        configSource.get(
            ConfigSource.class, "column_options", CONFIG_MAPPER_FACTORY.newConfigSource());
    columnOptions.set(columnName, columnOption);
    configSource.set("column_options", columnOptions);
    return configSource;
  }
}
