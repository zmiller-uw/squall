/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.utilities;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;

import java.io.Serializable;

public class SketchCountMin implements Serializable {

    ////////////////////////////////
    // Count-Min.
    ////////////////////////////////
    //
    // These should of course all be initialized in the constructor, not
    // hard-coded.  Prime numbers should be generated based on a seed.
    // Also, the dimension needs to be variable (_y) and not "3" as it is
    // currently implemented.

    public int _x = 997;
    public int _y = 3;

    public long[] _P1 = {5915587277L, 1500450271L, 3267000013L};
    public long[] _P2 = {5754853343L, 4093082899L, 9576890767L};
    public long[] _P3 = {3628273133L, 2860486313L, 5463458053L};
    public long[] _P4 = {13L,         17L,         19L        };

    // Actual sketch grid
    public long[][] _sketch = new long[_x][_y];

    public SketchCountMin(int x, int y) {
	_x = x;
	_y = 3;
	System.out.println("new SketchCountMin(" + _x + "," + _y + ") [paramter " + y + " ignored]");
    }


    public static int safeLongToInt(long l) {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
		throw new IllegalArgumentException
			(l + " cannot be cast to int without changing its value.");
	}
	int zz = (int) l;
	if(zz > 0) {
		return zz;
	} else {
		return -zz;
	}
    }


    public boolean AddToSketch(long k) {
        System.out.println("ADD SKETCH FOR " + k);

        for (int y = 0; y < _y; y++) {
            int x = safeLongToInt(((k * _P1[y] + _P2[y]) * _P3[y] + _P4[y]) % _x);

            System.out.println("C[" + x + "][" + y + "] ... ");
            System.out.println("C[" + x + "][" + y + "] was " + _sketch[x][y]);
            _sketch[x][y]++;
            System.out.println("C[" + x + "][" + y + "] now " + _sketch[x][y]);
        }
		return true;
    }


    public long GetSketchMin(long k) {
        System.out.println("GET SKETCH FOR " + k);

        int y = 0;
        int x = safeLongToInt(((k * _P1[y] + _P2[y]) * _P3[y] + _P4[y]) % _x);
        long v = _sketch[x][y];

        System.out.println("C[" + x + "][" + y + "] == " + _sketch[x][y]);

        for (y = 1; y < _y; y++) {
            x = safeLongToInt(((k * _P1[y] + _P2[y]) * _P3[y] + _P4[y]) % _x);
            System.out.println("C[" + x + "][" + y + "] == " + _sketch[x][y]);
            if (_sketch[x][y] < v) {
                v = _sketch[x][y];
            }
        }
        return v;
    }


    public long UpdateSketch(long k, int delta) {

        int y = 0;
        int x = 0;
        long min = -1;

	// for each hash function
        for (y = 0; y < _y; y++) {

	    // find which column to update
            x = safeLongToInt(((k * _P1[y] + _P2[y]) * _P3[y] + _P4[y]) % _x);

	    // keep the previous value (just for printing) and update
	    long prev = _sketch[x][y];
            _sketch[x][y] = prev + delta;

	    // debug information
            //System.out.println("C[" + x + "][" + y + "] was " + prev + " now " + _sketch[x][y]);

	    // is the new value the minimum so far?
            if ((min == -1) || (_sketch[x][y] < min)) {
                min = _sketch[x][y];
            }
        }
        System.out.println("UPDATE SKETCH FOR " + k + " DELTA " + delta + " RESULT " + min);
        return min;
    }

}
