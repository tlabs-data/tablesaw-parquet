package tech.tablesaw.io.parquet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestParquetReadOptions {

  @Test
  void testMissingValueIndicatorWarning() {
    final Appender appender = mock(Appender.class);
    Logger.getRootLogger().addAppender(appender);
    try {
      TablesawParquetReadOptions.builder("notused").missingValueIndicator("NA");
      ArgumentCaptor<LoggingEvent> argument = ArgumentCaptor.forClass(LoggingEvent.class);
      verify(appender).doAppend(argument.capture());
      final LoggingEvent event = argument.getValue();
      assertEquals(Level.WARN, event.getLevel());
      assertEquals("tech.tablesaw.io.parquet.TablesawParquetReadOptions", event.getLoggerName());
      assertEquals(
          "Missing value indicator is not used in TablesawParquetReadOptions", event.getMessage());
    } finally {
      Logger.getRootLogger().removeAppender(appender);
    }
  }

  @Test
  void testIgnoreZeroDecimalWarning() {
    final Appender appender = mock(Appender.class);
    Logger.getRootLogger().addAppender(appender);
    try {
      TablesawParquetReadOptions.builder("notused").ignoreZeroDecimal(true);
      ArgumentCaptor<LoggingEvent> argument = ArgumentCaptor.forClass(LoggingEvent.class);
      verify(appender).doAppend(argument.capture());
      final LoggingEvent event = argument.getValue();
      assertEquals(Level.WARN, event.getLevel());
      assertEquals("tech.tablesaw.io.parquet.TablesawParquetReadOptions", event.getLoggerName());
      assertEquals(
          "ignoreZeroDecimal has no effect in TablesawParquetReadOptions", event.getMessage());
    } finally {
      Logger.getRootLogger().removeAppender(appender);
    }
  }

  @Test
  void testSamplingWarning() {
    final Appender appender = mock(Appender.class);
    Logger.getRootLogger().addAppender(appender);
    try {
      TablesawParquetReadOptions.builder("notused").sample(true);
      ArgumentCaptor<LoggingEvent> argument = ArgumentCaptor.forClass(LoggingEvent.class);
      verify(appender).doAppend(argument.capture());
      final LoggingEvent event = argument.getValue();
      assertEquals(Level.WARN, event.getLevel());
      assertEquals("tech.tablesaw.io.parquet.TablesawParquetReadOptions", event.getLoggerName());
      assertEquals("Sampling is not used in TablesawParquetReadOptions", event.getMessage());
    } finally {
      Logger.getRootLogger().removeAppender(appender);
    }
  }
}
