package net.tlabs.jts.io;

/*-
 * #%L
 * Tablesaw-Parquet
 * %%
 * Copyright (C) 2020 - 2026 Tlabs-data
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
/*
 * Derivative work of the JTS library distributed with the following copyright notice:
 *  
 * Copyright (c) 2016 Vivid Solutions.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.EnumSet;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.Ordinate;
import org.locationtech.jts.io.OrdinateFormat;
import org.locationtech.jts.io.WKTConstants;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.util.Assert;

/*
 * Copyright (c) 2016 Vivid Solutions.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
/**
 * Writes the Well-Known Text representation of a {@link Geometry}. The
 * Well-Known Text format is defined in the OGC
 * <a href="http://www.opengis.org/techno/specs.htm"> <i>Simple Features
 * Specification for SQL</i></a>. See {@link WKTReader} for a formal
 * specification of the format syntax.
 * <p>
 * The <code>WKTWriter</code> outputs coordinates rounded to the precision
 * model. Only the maximum number of decimal places necessary to represent the
 * ordinates to the required precision will be output.
 * <p>
 * The SFS WKT spec does not define a special tag for {@link LinearRing}s. Under
 * the spec, rings are output as <code>LINESTRING</code>s. In order to allow
 * precisely specifying constructed geometries, JTS also supports a non-standard
 * <code>LINEARRING</code> tag which is used to output LinearRings.
 *
 * @version 1.7
 * @see WKTReader
 */
public class GenericWKTWriter {

    /**
     * A filter implementation to test if a coordinate sequence actually has
     * meaningful values for an ordinate bit-pattern
     */
    private class CheckOrdinatesFilter implements CoordinateSequenceFilter {

        private final EnumSet<Ordinate> checkOrdinateFlags;
        private final EnumSet<Ordinate> outputOrdinates;

        /**
         * Creates an instance of this class
         * 
         * @param checkOrdinateFlags the index for the ordinates to test.
         */
        private CheckOrdinatesFilter(final EnumSet<Ordinate> checkOrdinateFlags) {

            this.outputOrdinates = EnumSet.of(Ordinate.X, Ordinate.Y);
            this.checkOrdinateFlags = checkOrdinateFlags;
        }

        /** @see org.locationtech.jts.geom.CoordinateSequenceFilter#isGeometryChanged */
        @Override
        public void filter(final CoordinateSequence seq, final int i) {

            if (checkOrdinateFlags.contains(Ordinate.Z) && !Double.isNaN(seq.getZ(i))) {
                outputOrdinates.add(Ordinate.Z);
            }

            if (checkOrdinateFlags.contains(Ordinate.M) && !Double.isNaN(seq.getM(i))) {
                outputOrdinates.add(Ordinate.M);
            }
        }

        /** @see org.locationtech.jts.geom.CoordinateSequenceFilter#isGeometryChanged */
        @Override
        public boolean isGeometryChanged() {
            return false;
        }

        /** @see org.locationtech.jts.geom.CoordinateSequenceFilter#isDone */
        @Override
        public boolean isDone() {
            return outputOrdinates.equals(checkOrdinateFlags);
        }

        /**
         * Gets the evaluated ordinate bit-pattern
         *
         * @return A bit-pattern of ordinates with valid values masked by
         *         {@link #checkOrdinateFlags}.
         */
        EnumSet<Ordinate> getOutputOrdinates() {
            return outputOrdinates;
        }
    }

    private static final int INDENT = 2;
    private static final int OUTPUT_DIMENSION = 2;

    private static final String SPACE = " ";
    private static final String CLOSE_PARENS = ")";
    private static final String OPEN_PARENS = "(";
    private static final String COLON_SEPARATOR = ", ";
    private static final String EOL = "\n";

    private static final EnumSet<Ordinate> ORDINATE_SET_NONE_OF = EnumSet.noneOf(Ordinate.class);
    private static final EnumSet<Ordinate> ORDINATE_SET_Z = EnumSet.of(Ordinate.Z);
    private static final EnumSet<Ordinate> ORDINATE_SET_M = EnumSet.of(Ordinate.M);
    private static final EnumSet<Ordinate> ORDINATE_SET_ZM = EnumSet.of(Ordinate.Z, Ordinate.M);

    private EnumSet<Ordinate> outputOrdinates = EnumSet.of(Ordinate.X, Ordinate.Y);
    private int outputDimension = OUTPUT_DIMENSION;
    private OrdinateFormat ordinateFormat = null;
    private int coordsPerLine = -1;
    private String indentTabStr;

    public GenericWKTWriter() {
        super();
        setTab(INDENT);
    }

    /**
     * Sets the maximum number of coordinates per line written in formatted output.
     * If the provided coordinate number is &lt;= 0, coordinates will be written all
     * on one line.
     *
     * @param coordsPerLine the number of coordinates per line to output.
     */
    public void setMaxCoordinatesPerLine(final int coordsPerLine) {
        this.coordsPerLine = coordsPerLine;
    }

    /**
     * Sets the tab size to use for indenting.
     *
     * @param size the number of spaces to use as the tab string
     * @throws IllegalArgumentException if the size is non-positive
     */
    public void setTab(final int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Tab count must be positive");
        }
        this.indentTabStr = SPACE.repeat(size);
    }

    /**
     * Sets the {@link Ordinate} that are to be written. Possible members are:
     * <ul>
     * <li>{@link Ordinate#X}</li>
     * <li>{@link Ordinate#Y}</li>
     * <li>{@link Ordinate#Z}</li>
     * <li>{@link Ordinate#M}</li>
     * </ul>
     * Values of {@link Ordinate#X} and {@link Ordinate#Y} are always assumed and
     * not particularly checked for.
     *
     * @param outputOrdinates A set of {@link Ordinate} values
     */
    public void setOutputOrdinates(final EnumSet<Ordinate> outputOrdinates) {

        this.outputOrdinates.remove(Ordinate.Z);
        this.outputOrdinates.remove(Ordinate.M);

        if (this.outputDimension == 3) {
            if (outputOrdinates.contains(Ordinate.Z)) {
                this.outputOrdinates.add(Ordinate.Z);
            } else if (outputOrdinates.contains(Ordinate.M)) {
                this.outputOrdinates.add(Ordinate.M);
            }
        }
        if (this.outputDimension == 4) {
            if (outputOrdinates.contains(Ordinate.Z)) {
                this.outputOrdinates.add(Ordinate.Z);
            }
            if (outputOrdinates.contains(Ordinate.M)) {
                this.outputOrdinates.add(Ordinate.M);
            }
        }
    }

    // Adapted from https://github.com/locationtech/jts/pull/1220
    /**
     * Computes the coordinate dimension of a <code>Geometry</code>, based on the
     * coordinate dimensions of its {@link CoordinateSequence}s (i.e. 2 for XY, 3
     * for XYZ or XYM, or 4 for XYZM). For a {@link GeometryCollection} this is the
     * maximum coordinate dimension over all of its elements. Empty geometries (or
     * empty elements of a collection) are assumed to have coordinate dimension 2,
     * and do not affect the result unless all elements are empty.
     *
     * @param g the <code>Geometry</code> to get the coordinate dimension of
     * @return the coordinate dimension of the geometry (2, 3 or 4)
     */
    public static int getCoordinateDimension(final Geometry g) {
        if (g instanceof Point) {
            return coordinateSequenceDimension(((Point) g).getCoordinateSequence());
        }
        if (g instanceof LineString) {
            return coordinateSequenceDimension(((LineString) g).getCoordinateSequence());
        }
        if (g instanceof Polygon) {
            final Polygon poly = (Polygon) g;
            int dimension = 2;
            final LinearRing shell = poly.getExteriorRing();
            if (shell != null) {
                dimension = coordinateSequenceDimension(shell.getCoordinateSequence());
            }
            for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                dimension = Math.max(dimension,
                    coordinateSequenceDimension(poly.getInteriorRingN(i).getCoordinateSequence()));
            }
            return dimension;
        }
        if (g instanceof GeometryCollection) {
            final GeometryCollection gc = (GeometryCollection) g;
            int dimension = 2;
            for (int i = 0; i < gc.getNumGeometries(); i++) {
                dimension = Math.max(dimension, getCoordinateDimension(gc.getGeometryN(i)));
            }
            return dimension;
        }
        return 2;
    }

    private static int coordinateSequenceDimension(final CoordinateSequence seq) {
        return seq.getDimension();
    }

    public static int getCoordinateMeasure(final Geometry g) {
        if (g instanceof Point) {
            return coordinateSequenceMeasure(((Point) g).getCoordinateSequence());
        }
        if (g instanceof LineString) {
            return coordinateSequenceMeasure(((LineString) g).getCoordinateSequence());
        }
        if (g instanceof Polygon) {
            final Polygon poly = (Polygon) g;
            int measure = 0;
            final LinearRing shell = poly.getExteriorRing();
            if (shell != null) {
                measure = coordinateSequenceMeasure(shell.getCoordinateSequence());
            }
            for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                measure = Math.max(measure,
                    coordinateSequenceMeasure(poly.getInteriorRingN(i).getCoordinateSequence()));
            }
            return measure;
        }
        if (g instanceof GeometryCollection) {
            final GeometryCollection gc = (GeometryCollection) g;
            int measure = 0;
            for (int i = 0; i < gc.getNumGeometries(); i++) {
                measure = Math.max(measure, getCoordinateMeasure(gc.getGeometryN(i)));
            }
            return measure;
        }
        return 0;
    }

    private static int coordinateSequenceMeasure(final CoordinateSequence seq) {
        return seq.getMeasures();
    }

    /**
     * Converts a <code>Geometry</code> to its Well-known Text representation.
     *
     * @param geometry a <code>Geometry</code> to process
     * @return a &lt;Geometry Tagged Text&gt; string (see the OpenGIS Simple
     *         Features Specification)
     */
    public String write(final Geometry geometry) {
        final Writer sw = new StringWriter();

        try {
            writeFormatted(geometry, false, sw);
        } catch (IOException ex) {
            Assert.shouldNeverReachHere();
        }
        return sw.toString();
    }

    /**
     * Same as <code>write</code>, but with newlines and spaces to make the
     * well-known text more readable.
     *
     * @param geometry a <code>Geometry</code> to process
     * @return a &lt;Geometry Tagged Text&gt; string (see the OpenGIS Simple
     *         Features Specification), with newlines and spaces
     */
    public String writeFormatted(final Geometry geometry) {
        final Writer sw = new StringWriter();
        try {
            writeFormatted(geometry, true, sw);
        } catch (IOException ex) {
            Assert.shouldNeverReachHere();
        }
        return sw.toString();
    }

    /**
     * Converts a <code>Geometry</code> to its Well-known Text representation.
     *
     * @param geometry a <code>Geometry</code> to process
     */
    private void writeFormatted(final Geometry geometry, final boolean useFormatting, final Writer writer)
        throws IOException {
        final OrdinateFormat formatter = getFormatter(geometry);
        // changed: adapt dimension to Geometry
        this.outputDimension = getCoordinateDimension(geometry);
        EnumSet<Ordinate> additionalOrdinate = ORDINATE_SET_NONE_OF;
        switch (outputDimension) {
            case 2:
                break;
            case 3:
                if (getCoordinateMeasure(geometry) > 0) {
                    additionalOrdinate = ORDINATE_SET_M;
                } else {
                    additionalOrdinate = ORDINATE_SET_Z;
                }
                break;
            case 4:
                additionalOrdinate = ORDINATE_SET_ZM;
                break;
            default:
                break;
        }
        this.setOutputOrdinates(additionalOrdinate);
        // append the WKT
        appendGeometryTaggedText(geometry, useFormatting, writer, formatter, additionalOrdinate);
    }

    private OrdinateFormat getFormatter(final Geometry geometry) {
        // if present use the cached formatter
        if (ordinateFormat != null) {
            return ordinateFormat;
        }
        // no precision model was specified, so use the geometry's and cache formatter
        ordinateFormat = createFormatter(geometry.getPrecisionModel());
        return ordinateFormat;
    }

    /**
     * Creates the <code>DecimalFormat</code> used to write <code>double</code>s
     * with a sufficient number of decimal places.
     *
     * @param precisionModel the <code>PrecisionModel</code> used to determine the
     *                       number of decimal places to write.
     * @return a <code>DecimalFormat</code> that write <code>double</code> s without
     *         scientific notation.
     */
    private static OrdinateFormat createFormatter(final PrecisionModel precisionModel) {
        return OrdinateFormat.create(precisionModel.getMaximumSignificantDigits());
    }

    /**
     * Converts a <code>Geometry</code> to &lt;Geometry Tagged Text&gt; format, then
     * appends it to the writer.
     *
     * @param geometry           the <code>Geometry</code> to process
     * @param useFormatting      flag indicating that the output should be formatted
     * @param writer             the output writer to append to
     * @param formatter          the <code>DecimalFormatter</code> to use to convert
     *                           from a precise coordinate to an external coordinate
     * @param additionalOrdinate
     */
    private void appendGeometryTaggedText(final Geometry geometry, final boolean useFormatting, final Writer writer,
        final OrdinateFormat formatter, final EnumSet<Ordinate> additionalOrdinate) throws IOException {
        // evaluate the ordinates actually present in the geometry
        final CheckOrdinatesFilter cof = new CheckOrdinatesFilter(this.outputOrdinates);
        geometry.apply(cof);

        // Append the WKT
        appendGeometryTaggedText(geometry, cof.getOutputOrdinates(), additionalOrdinate, useFormatting, 0, writer,
            formatter);
    }

    /**
     * Converts a <code>Geometry</code> to &lt;Geometry Tagged Text&gt; format, then
     * appends it to the writer.
     *
     * @param geometry           the <code>Geometry</code> to process
     * @param additionalOrdinate
     * @param useFormatting      flag indicating that the output should be formatted
     * @param level              the indentation level
     * @param writer             the output writer to append to
     * @param formatter          the <code>DecimalFormatter</code> to use to convert
     *                           from a precise coordinate to an external coordinate
     */
    private void appendGeometryTaggedText(final Geometry geometry, final EnumSet<Ordinate> outputOrdinates,
        final EnumSet<Ordinate> additionalOrdinate, final boolean useFormatting, final int level, final Writer writer,
        final OrdinateFormat formatter) throws IOException

    {
        indent(useFormatting, level, writer);

        if (geometry instanceof Point) {
            appendPointTaggedText((Point) geometry, outputOrdinates, additionalOrdinate, useFormatting, level, writer,
                formatter);
        } else if (geometry instanceof LinearRing) {
            appendLinearRingTaggedText((LinearRing) geometry, outputOrdinates, additionalOrdinate, useFormatting, level,
                writer, formatter);
        } else if (geometry instanceof LineString) {
            appendLineStringTaggedText((LineString) geometry, outputOrdinates, additionalOrdinate, useFormatting, level,
                writer, formatter);
        } else if (geometry instanceof Polygon) {
            appendPolygonTaggedText((Polygon) geometry, outputOrdinates, additionalOrdinate, useFormatting, level,
                writer, formatter);
        } else if (geometry instanceof MultiPoint) {
            appendMultiPointTaggedText((MultiPoint) geometry, outputOrdinates, additionalOrdinate, useFormatting, level,
                writer, formatter);
        } else if (geometry instanceof MultiLineString) {
            appendMultiLineStringTaggedText((MultiLineString) geometry, outputOrdinates, additionalOrdinate,
                useFormatting, level, writer, formatter);
        } else if (geometry instanceof MultiPolygon) {
            appendMultiPolygonTaggedText((MultiPolygon) geometry, outputOrdinates, additionalOrdinate, useFormatting,
                level, writer, formatter);
        } else if (geometry instanceof GeometryCollection) {
            appendGeometryCollectionTaggedText((GeometryCollection) geometry, outputOrdinates, additionalOrdinate,
                useFormatting, level, writer, formatter);
        } else {
            Assert.shouldNeverReachHere("Unsupported Geometry implementation:" + geometry.getClass());
        }
    }

    /**
     * Converts a <code>Coordinate</code> to &lt;Point Tagged Text&gt; format, then
     * appends it to the writer.
     *
     * @param point              the <code>Point</code> to process
     * @param additionalOrdinate
     * @param useFormatting      flag indicating that the output should be formatted
     * @param level              the indentation level
     * @param writer             the output writer to append to
     * @param formatter          the formatter to use when writing numbers
     */
    private void appendPointTaggedText(final Point point, final EnumSet<Ordinate> outputOrdinates,
        final EnumSet<Ordinate> additionalOrdinate, final boolean useFormatting, final int level, final Writer writer,
        final OrdinateFormat formatter) throws IOException {
        writer.write(WKTConstants.POINT);
        writer.write(SPACE);
        appendOrdinateText(additionalOrdinate, writer);
        appendSequenceText(point.getCoordinateSequence(), outputOrdinates, useFormatting, level, false, writer,
            formatter);
    }

    /**
     * Converts a <code>LineString</code> to &lt;LineString Tagged Text&gt; format,
     * then appends it to the writer.
     *
     * @param lineString         the <code>LineString</code> to process
     * @param additionalOrdinate
     * @param useFormatting      flag indicating that the output should be formatted
     * @param level              the indentation level
     * @param writer             the output writer to append to
     * @param formatter          the <code>DecimalFormatter</code> to use to convert
     *                           from a precise coordinate to an external coordinate
     */
    private void appendLineStringTaggedText(final LineString lineString, final EnumSet<Ordinate> outputOrdinates,
        final EnumSet<Ordinate> additionalOrdinate, final boolean useFormatting, final int level, final Writer writer,
        final OrdinateFormat formatter) throws IOException {
        writer.write(WKTConstants.LINESTRING);
        writer.write(SPACE);
        appendOrdinateText(additionalOrdinate, writer);
        appendSequenceText(lineString.getCoordinateSequence(), outputOrdinates, useFormatting, level, false, writer,
            formatter);
    }

    /**
     * Converts a <code>LinearRing</code> to &lt;LinearRing Tagged Text&gt; format,
     * then appends it to the writer.
     *
     * @param linearRing         the <code>LinearRing</code> to process
     * @param additionalOrdinate
     * @param useFormatting      flag indicating that the output should be formatted
     * @param level              the indentation level
     * @param writer             the output writer to append to
     * @param formatter          the <code>DecimalFormatter</code> to use to convert
     *                           from a precise coordinate to an external coordinate
     */
    private void appendLinearRingTaggedText(final LinearRing linearRing, final EnumSet<Ordinate> outputOrdinates,
        final EnumSet<Ordinate> additionalOrdinate, final boolean useFormatting, final int level, final Writer writer,
        final OrdinateFormat formatter) throws IOException {
        writer.write(WKTConstants.LINEARRING);
        writer.write(SPACE);
        appendOrdinateText(additionalOrdinate, writer);
        appendSequenceText(linearRing.getCoordinateSequence(), outputOrdinates, useFormatting, level, false, writer,
            formatter);
    }

    /**
     * Converts a <code>Polygon</code> to &lt;Polygon Tagged Text&gt; format, then
     * appends it to the writer.
     *
     * @param polygon            the <code>Polygon</code> to process
     * @param additionalOrdinate
     * @param useFormatting      flag indicating that the output should be formatted
     * @param level              the indentation level
     * @param writer             the output writer to append to
     * @param formatter          the <code>DecimalFormatter</code> to use to convert
     *                           from a precise coordinate to an external coordinate
     */
    private void appendPolygonTaggedText(final Polygon polygon, final EnumSet<Ordinate> outputOrdinates,
        final EnumSet<Ordinate> additionalOrdinate, final boolean useFormatting, final int level, final Writer writer,
        final OrdinateFormat formatter) throws IOException {
        writer.write(WKTConstants.POLYGON);
        writer.write(SPACE);
        appendOrdinateText(additionalOrdinate, writer);
        appendPolygonText(polygon, outputOrdinates, useFormatting, level, false, writer, formatter);
    }

    /**
     * Converts a <code>MultiPoint</code> to &lt;MultiPoint Tagged Text&gt; format,
     * then appends it to the writer.
     *
     * @param multipoint         the <code>MultiPoint</code> to process
     * @param additionalOrdinate
     * @param useFormatting      flag indicating that the output should be formatted
     * @param level              the indentation level
     * @param writer             the output writer to append to
     * @param formatter          the <code>DecimalFormatter</code> to use to convert
     *                           from a precise coordinate to an external coordinate
     */
    private void appendMultiPointTaggedText(final MultiPoint multipoint, final EnumSet<Ordinate> outputOrdinates,
        final EnumSet<Ordinate> additionalOrdinate, final boolean useFormatting, final int level, final Writer writer,
        final OrdinateFormat formatter) throws IOException {
        writer.write(WKTConstants.MULTIPOINT);
        writer.write(SPACE);
        appendOrdinateText(additionalOrdinate, writer);
        appendMultiPointText(multipoint, outputOrdinates, useFormatting, level, writer, formatter);
    }

    /**
     * Converts a <code>MultiLineString</code> to &lt;MultiLineString Tagged
     * Text&gt; format, then appends it to the writer.
     *
     * @param multiLineString    the <code>MultiLineString</code> to process
     * @param additionalOrdinate
     * @param useFormatting      flag indicating that the output should be formatted
     * @param level              the indentation level
     * @param writer             the output writer to append to
     * @param formatter          the <code>DecimalFormatter</code> to use to convert
     *                           from a precise coordinate to an external coordinate
     */
    private void appendMultiLineStringTaggedText(final MultiLineString multiLineString,
        final EnumSet<Ordinate> outputOrdinates, final EnumSet<Ordinate> additionalOrdinate,
        final boolean useFormatting, final int level, final Writer writer, final OrdinateFormat formatter)
        throws IOException {
        writer.write(WKTConstants.MULTILINESTRING);
        writer.write(SPACE);
        appendOrdinateText(additionalOrdinate, writer);
        appendMultiLineStringText(multiLineString, outputOrdinates, useFormatting, level, /* false, */writer,
            formatter);
    }

    /**
     * Converts a <code>MultiPolygon</code> to &lt;MultiPolygon Tagged Text&gt;
     * format, then appends it to the writer.
     *
     * @param multiPolygon       the <code>MultiPolygon</code> to process
     * @param additionalOrdinate
     * @param useFormatting      flag indicating that the output should be formatted
     * @param level              the indentation level
     * @param writer             the output writer to append to
     * @param formatter          the <code>DecimalFormatter</code> to use to convert
     *                           from a precise coordinate to an external coordinate
     */
    private void appendMultiPolygonTaggedText(final MultiPolygon multiPolygon, final EnumSet<Ordinate> outputOrdinates,
        final EnumSet<Ordinate> additionalOrdinate, final boolean useFormatting, final int level, final Writer writer,
        final OrdinateFormat formatter) throws IOException {
        writer.write(WKTConstants.MULTIPOLYGON);
        writer.write(SPACE);
        appendOrdinateText(additionalOrdinate, writer);
        appendMultiPolygonText(multiPolygon, outputOrdinates, useFormatting, level, writer, formatter);
    }

    /**
     * Converts a <code>GeometryCollection</code> to &lt;GeometryCollection Tagged
     * Text&gt; format, then appends it to the writer.
     *
     * @param geometryCollection the <code>GeometryCollection</code> to process
     * @param additionalOrdinate
     * @param useFormatting      flag indicating that the output should be formatted
     * @param level              the indentation level
     * @param writer             the output writer to append to
     * @param formatter          the <code>DecimalFormatter</code> to use to convert
     *                           from a precise coordinate to an external coordinate
     */
    private void appendGeometryCollectionTaggedText(final GeometryCollection geometryCollection,
        final EnumSet<Ordinate> outputOrdinates, final EnumSet<Ordinate> additionalOrdinate,
        final boolean useFormatting, final int level, final Writer writer, final OrdinateFormat formatter)
        throws IOException {
        writer.write(WKTConstants.GEOMETRYCOLLECTION);
        writer.write(SPACE);
        appendOrdinateText(additionalOrdinate, writer);
        appendGeometryCollectionText(geometryCollection, outputOrdinates, additionalOrdinate, useFormatting, level,
            writer, formatter);
    }

    /**
     * Appends the i'th coordinate from the sequence to the writer
     * <p>
     * If the {@code seq} has coordinates that are {@link double.NAN}, these are not
     * written, even though {@link #outputDimension} suggests this.
     *
     * @param seq       the <code>CoordinateSequence</code> to process
     * @param i         the index of the coordinate to write
     * @param writer    the output writer to append to
     * @param formatter the formatter to use for writing ordinate values
     */
    private static void appendCoordinate(final CoordinateSequence seq, final EnumSet<Ordinate> outputOrdinates,
        final int i, final Writer writer, final OrdinateFormat formatter) throws IOException {
        writer.write(writeNumber(seq.getX(i), formatter));
        writer.write(SPACE);
        writer.write(writeNumber(seq.getY(i), formatter));

        if (outputOrdinates.contains(Ordinate.Z)) {
            writer.write(SPACE);
            writer.write(writeNumber(seq.getZ(i), formatter));
        }

        if (outputOrdinates.contains(Ordinate.M)) {
            writer.write(SPACE);
            writer.write(writeNumber(seq.getM(i), formatter));
        }
    }

    /**
     * Converts a <code>double</code> to a <code>String</code>, not in scientific
     * notation.
     *
     * @param d the <code>double</code> to convert
     * @return the <code>double</code> as a <code>String</code>, not in scientific
     *         notation
     */
    private static String writeNumber(final double d, final OrdinateFormat formatter) {
        return formatter.format(d);
    }

    /**
     * Appends additional ordinate information. This function may
     * <ul>
     * <li>append 'Z' if in {@code outputOrdinates} the {@link Ordinate#Z} value is
     * included</li>
     * <li>append 'M' if in {@code outputOrdinates} the {@link Ordinate#M} value is
     * included</li>
     * <li>append 'ZM' if in {@code outputOrdinates} the {@link Ordinate#Z} and
     * {@link Ordinate#M} values are included</li>
     * </ul>
     *
     * @param outputOrdinates a bit-pattern of ordinates to write.
     * @param writer          the output writer to append to.
     * @throws IOException if an error occurs while using the writer.
     */
    private static void appendOrdinateText(final EnumSet<Ordinate> outputOrdinates, final Writer writer)
        throws IOException {
        // Z
        if (outputOrdinates.contains(Ordinate.Z)) {
            writer.append(WKTConstants.Z);
            // ZM
            if (outputOrdinates.contains(Ordinate.M)) {
                writer.append(WKTConstants.M);
            }
            writer.append(SPACE);
            // M
        } else if (outputOrdinates.contains(Ordinate.M)) {
            writer.append(WKTConstants.M);
            writer.append(SPACE);
        }
    }

    /**
     * Appends all members of a <code>CoordinateSequence</code> to the stream. Each
     * {@code Coordinate} is separated from another using a colon, the ordinates of
     * a {@code Coordinate} are separated by a space.
     *
     * @param seq           the <code>CoordinateSequence</code> to process
     * @param useFormatting flag indicating that
     * @param level         the indentation level
     * @param indentFirst   flag indicating that the first {@code Coordinate} of the
     *                      sequence should be indented for better visibility
     * @param writer        the output writer to append to
     * @param formatter     the formatter to use for writing ordinate values.
     */
    private void appendSequenceText(final CoordinateSequence seq, final EnumSet<Ordinate> outputOrdinates,
        final boolean useFormatting, final int level, final boolean indentFirst, final Writer writer,
        final OrdinateFormat formatter) throws IOException {
        if (seq.size() == 0) {
            writer.write(WKTConstants.EMPTY);
        } else {
            if (indentFirst) {
                indent(useFormatting, level, writer);
            }
            writer.write(OPEN_PARENS);
            for (int i = 0; i < seq.size(); i++) {
                if (i > 0) {
                    writer.write(COLON_SEPARATOR);
                    if (coordsPerLine > 0 && i % coordsPerLine == 0) {
                        indent(useFormatting, level + 1, writer);
                    }
                }
                appendCoordinate(seq, outputOrdinates, i, writer, formatter);
            }
            writer.write(CLOSE_PARENS);
        }
    }

    /**
     * Converts a <code>Polygon</code> to &lt;Polygon Text&gt; format, then appends
     * it to the writer.
     *
     * @param polygon       the <code>Polygon</code> to process
     * @param useFormatting flag indicating that
     * @param level         the indentation level
     * @param indentFirst   flag indicating that the first {@code Coordinate} of the
     *                      sequence should be indented for better visibility
     * @param writer        the output writer to append to
     * @param formatter     the formatter to use for writing ordinate values.
     */
    private void appendPolygonText(final Polygon polygon, final EnumSet<Ordinate> outputOrdinates,
        final boolean useFormatting, final int level, final boolean indentFirst, final Writer writer,
        final OrdinateFormat formatter) throws IOException {
        if (polygon.isEmpty()) {
            writer.write(WKTConstants.EMPTY);
        } else {
            if (indentFirst) {
                indent(useFormatting, level, writer);
            }
            writer.write(OPEN_PARENS);
            appendSequenceText(polygon.getExteriorRing().getCoordinateSequence(), outputOrdinates, useFormatting, level,
                false, writer, formatter);
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                writer.write(COLON_SEPARATOR);
                appendSequenceText(polygon.getInteriorRingN(i).getCoordinateSequence(), outputOrdinates, useFormatting,
                    level + 1, true, writer, formatter);
            }
            writer.write(CLOSE_PARENS);
        }
    }

    /**
     * Converts a <code>MultiPoint</code> to &lt;MultiPoint Text&gt; format, then
     * appends it to the writer.
     *
     * @param multiPoint    the <code>MultiPoint</code> to process
     * @param useFormatting flag indicating that
     * @param level         the indentation level
     * @param writer        the output writer to append to
     * @param formatter     the formatter to use for writing ordinate values.
     */
    private void appendMultiPointText(final MultiPoint multiPoint, final EnumSet<Ordinate> outputOrdinates,
        final boolean useFormatting, final int level, Writer writer, final OrdinateFormat formatter)
        throws IOException {
        if (multiPoint.getNumGeometries() == 0) {
            writer.write(WKTConstants.EMPTY);
        } else {
            writer.write(OPEN_PARENS);
            for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
                if (i > 0) {
                    writer.write(COLON_SEPARATOR);
                    indentCoords(useFormatting, i, level + 1, writer);
                }
                appendSequenceText(((Point) multiPoint.getGeometryN(i)).getCoordinateSequence(), outputOrdinates,
                    useFormatting, level, false, writer, formatter);
            }
            writer.write(CLOSE_PARENS);
        }
    }

    /**
     * Converts a <code>MultiLineString</code> to &lt;MultiLineString Text&gt;
     * format, then appends it to the writer.
     *
     * @param multiLineString the <code>MultiLineString</code> to process
     * @param useFormatting   flag indicating that
     * @param level           the indentation level //@param indentFirst flag
     *                        indicating that the first {@code Coordinate} of the
     *                        sequence should be indented for // better visibility
     * @param writer          the output writer to append to
     * @param formatter       the formatter to use for writing ordinate values.
     */
    private void appendMultiLineStringText(final MultiLineString multiLineString,
        final EnumSet<Ordinate> outputOrdinates, final boolean useFormatting, final int level, final Writer writer,
        final OrdinateFormat formatter) throws IOException {
        if (multiLineString.getNumGeometries() == 0) {
            writer.write(WKTConstants.EMPTY);
        } else {
            int level2 = level;
            boolean doIndent = false;
            writer.write(OPEN_PARENS);
            for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
                if (i > 0) {
                    writer.write(COLON_SEPARATOR);
                    level2 = level + 1;
                    doIndent = true;
                }
                appendSequenceText(((LineString) multiLineString.getGeometryN(i)).getCoordinateSequence(),
                    outputOrdinates, useFormatting, level2, doIndent, writer, formatter);
            }
            writer.write(CLOSE_PARENS);
        }
    }

    /**
     * Converts a <code>MultiPolygon</code> to &lt;MultiPolygon Text&gt; format,
     * then appends it to the writer.
     *
     * @param multiPolygon  the <code>MultiPolygon</code> to process
     * @param useFormatting flag indicating that
     * @param level         the indentation level
     * @param writer        the output writer to append to
     * @param formatter     the formatter to use for writing ordinate values.
     */
    private void appendMultiPolygonText(final MultiPolygon multiPolygon, final EnumSet<Ordinate> outputOrdinates,
        final boolean useFormatting, final int level, final Writer writer, final OrdinateFormat formatter)
        throws IOException {
        if (multiPolygon.getNumGeometries() == 0) {
            writer.write(WKTConstants.EMPTY);
        } else {
            int level2 = level;
            boolean doIndent = false;
            writer.write(OPEN_PARENS);
            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                if (i > 0) {
                    writer.write(COLON_SEPARATOR);
                    level2 = level + 1;
                    doIndent = true;
                }
                appendPolygonText((Polygon) multiPolygon.getGeometryN(i), outputOrdinates, useFormatting, level2,
                    doIndent, writer, formatter);
            }
            writer.write(CLOSE_PARENS);
        }
    }

    /**
     * Converts a <code>GeometryCollection</code> to &lt;GeometryCollectionText&gt;
     * format, then appends it to the writer.
     *
     * @param geometryCollection the <code>GeometryCollection</code> to process
     * @param additionalOrdinate
     * @param useFormatting      flag indicating that
     * @param level              the indentation level
     * @param writer             the output writer to append to
     * @param formatter          the formatter to use for writing ordinate values.
     */
    private void appendGeometryCollectionText(final GeometryCollection geometryCollection,
        final EnumSet<Ordinate> outputOrdinates, final EnumSet<Ordinate> additionalOrdinate,
        final boolean useFormatting, final int level, final Writer writer, final OrdinateFormat formatter)
        throws IOException {
        if (geometryCollection.getNumGeometries() == 0) {
            writer.write(WKTConstants.EMPTY);
        } else {
            int level2 = level;
            writer.write(OPEN_PARENS);
            for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
                if (i > 0) {
                    writer.write(COLON_SEPARATOR);
                    level2 = level + 1;
                }
                appendGeometryTaggedText(geometryCollection.getGeometryN(i), outputOrdinates, additionalOrdinate,
                    useFormatting, level2, writer, formatter);
            }
            writer.write(CLOSE_PARENS);
        }
    }

    private void indentCoords(final boolean useFormatting, final int coordIndex, final int level, final Writer writer)
        throws IOException {
        if (coordsPerLine <= 0 || coordIndex % coordsPerLine != 0) {
            return;
        }
        indent(useFormatting, level, writer);
    }

    private void indent(final boolean useFormatting, final int level, final Writer writer) throws IOException {
        if (!useFormatting || level <= 0) {
            return;
        }
        writer.write(EOL);
        for (int i = 0; i < level; i++) {
            writer.write(indentTabStr);
        }
    }

}
