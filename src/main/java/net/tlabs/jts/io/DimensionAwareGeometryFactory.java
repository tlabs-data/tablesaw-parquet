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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

class DimensionAwareGeometryFactory extends GeometryFactory {

    private static final long serialVersionUID = 1L;
    private DimensionAwareSequenceFactory dasFactory;

    public DimensionAwareGeometryFactory() {
        super(new DimensionAwareSequenceFactory());
        this.dasFactory =  (DimensionAwareSequenceFactory)getCoordinateSequenceFactory();
    }

    public DimensionAwareSequenceFactory getDimensionAwareSequenceFactory () {
        return dasFactory;
    }

    public Point createPoint(final int dimension, final int measure) {
        return createPoint(getDimensionAwareSequenceFactory().create(new Coordinate[]{}, dimension, measure));
    }

    public Polygon createPolygon(final int dimension, final int measure) {
        return createPolygon(getDimensionAwareSequenceFactory().create(new Coordinate[]{}, dimension, measure));
    }
    
    
}
