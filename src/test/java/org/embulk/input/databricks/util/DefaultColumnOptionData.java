package org.embulk.input.databricks.util;

import java.time.ZoneId;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.modules.ZoneIdModule;

public class DefaultColumnOptionData {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder()
          .addDefaultModules()
          .addModule(ZoneIdModule.withLegacyNames())
          .build();

  final String jdbcType;
  final String type;
  final String valueType;
  final String timestampFormat;
  final ZoneId timeZone;

  private DefaultColumnOptionData(
      String jdbcType, String type, String valueType, String timestampFormat, ZoneId timeZone) {
    this.jdbcType = jdbcType;
    this.type = type;
    this.valueType = valueType;
    this.timestampFormat = timestampFormat;
    this.timeZone = timeZone;
  }

  public static DefaultColumnOptionData create(String jdbcType, String type, String valueType) {
    return new DefaultColumnOptionData(jdbcType, type, valueType, null, null);
  }

  public static DefaultColumnOptionData create(
      String jdbcType, String valueType, String timestampFormat, ZoneId timeZone) {
    return new DefaultColumnOptionData(jdbcType, "string", valueType, timestampFormat, timeZone);
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
            ConfigSource.class, "default_column_options", CONFIG_MAPPER_FACTORY.newConfigSource());
    columnOptions.set(jdbcType, columnOption);
    configSource.set("default_column_options", columnOptions);
    return configSource;
  }
}
