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
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

class DimensionAwareSequenceFactory implements CoordinateSequenceFactory {

    /**
     * Returns a {@link CoordinateArraySequence} based on the given array (the array is
     * not copied).
     *
     * @param coordinates
     *            the coordinates, which may not be null nor contain null
     *            elements
     */
    @Override
    public CoordinateSequence create(final Coordinate[] coordinates) {
      return new CoordinateArraySequence(coordinates);
    }

    /**
     * @see org.locationtech.jts.geom.CoordinateSequenceFactory#create(org.locationtech.jts.geom.CoordinateSequence)
     */
    @Override
    public CoordinateSequence create(final CoordinateSequence coordSeq) {
      return new CoordinateArraySequence(coordSeq);
    }

    /**
     * The created sequence dimension is clamped to be &lt;= 3.
     * 
     * @see org.locationtech.jts.geom.CoordinateSequenceFactory#create(int, int)
     *
     */
    @Override
    public CoordinateSequence create(final int size, int dimension) {
      if (dimension > 3)
        dimension = 3;
      
      // handle bogus dimension
      if (dimension < 2)
        dimension = 2;      
      
      return new CoordinateArraySequence(size, dimension);
    }
    
    @Override
    public CoordinateSequence create(final int size, final int dimension, int measures) {
      int spatial = dimension - measures;
      
      if (measures > 1) {
        measures = 1; // clip measures
      }
      if ((spatial) > 3) {
        spatial = 3; // clip spatial dimension
      }
      
      if (spatial < 2)
        spatial = 2; // handle bogus spatial dimension
      
      return new CoordinateArraySequence(size, spatial + measures, measures);
    }

    public CoordinateSequence create(final Coordinate[] coordinates, final int dimension, final int measure) {
        return new CoordinateArraySequence(coordinates, dimension, measure);
    }

}
