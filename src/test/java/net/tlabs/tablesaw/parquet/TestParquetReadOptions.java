package net.tlabs.tablesaw.parquet;

/*-
 * #%L
 * Tablesaw-Parquet
 * %%
 * Copyright (C) 2020 - 2021 Tlabs-data
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
            assertEquals("net.tlabs.tablesaw.parquet.TablesawParquetReadOptions", event.getLoggerName());
            assertEquals("Missing value indicator is not used in TablesawParquetReadOptions", event.getMessage());
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
            assertEquals("net.tlabs.tablesaw.parquet.TablesawParquetReadOptions", event.getLoggerName());
            assertEquals("ignoreZeroDecimal has no effect in TablesawParquetReadOptions", event.getMessage());
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
            assertEquals("net.tlabs.tablesaw.parquet.TablesawParquetReadOptions", event.getLoggerName());
            assertEquals("Sampling is not used in TablesawParquetReadOptions", event.getMessage());
        } finally {
            Logger.getRootLogger().removeAppender(appender);
        }
    }
    
    @Test
    void testSingleColumn() {
        final TablesawParquetReadOptions options = TablesawParquetReadOptions.builder("notused")
            .withOnlyTheseColumns("col1").build();
        assertEquals(1, options.getColumns().size(), "Wrong columns list size");
    }
    
    @Test
    void testMultipleColumn() {
        final TablesawParquetReadOptions options = TablesawParquetReadOptions.builder("notused")
            .withOnlyTheseColumns("col1", "col2", "col3").build();
        assertEquals(3, options.getColumns().size(), "Wrong columns list size");
    }
    
    @Test
    void testColumnArray() {
        final TablesawParquetReadOptions options = TablesawParquetReadOptions.builder("notused")
            .withOnlyTheseColumns(new String[] {"col1", "col2", "col3"}).build();
        assertEquals(3, options.getColumns().size(), "Wrong columns list size");
    }
    
    @Test
    void testNoColumnFilter() {
        final TablesawParquetReadOptions options = TablesawParquetReadOptions.builder("notused").build();
        assertTrue(options.hasColumn("col1"), "Unfiltered column filtered out");
    }
    
    @Test
    void testColumnFilter() {
        final TablesawParquetReadOptions options = TablesawParquetReadOptions.builder("notused")
            .withOnlyTheseColumns("col1").build();
        assertTrue(options.hasColumn("col1"), "Kept column filtered out");
        assertFalse(options.hasColumn("col2"), "Filtered column kept");
    }
}
