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
    // This implementation has a table of large primes pre-loaded to implement
    // hash functions.  Hashes are computed through a series of multiplcations
    // and additions of the hash key with prime numbers from the table.  This
    // should be quick to compute as well as evenly distributed (since all the
    // numbers are prime, even if the key or the _x dimenstion is not).

    // enough primes to do up to 10 hash functions
    public long[] _P1 = {3579246841L, 5915587277L, 9012345697L, 7777777781L, 1500450271L, 5678901247L, 3267000013L, 1111111121L,  123456791L, 2468013631L};
    public long[] _P2 = {5754853343L, 4093082899L, 6666666757L, 9024681401L, 2222222243L, 9576890767L, 2345678917L, 4680135793L, 5555555557L, 6789012361L};
    public long[] _P3 = {3628273133L, 3333333403L, 2860486313L, 5463458053L, 1357902487L, 9999999769L, 3456789019L, 6801357929L, 5792468029L, 7890123473L};
    public long[] _P4 = {4444444447L, 3141592661L, 1448762431L, 8675309129L, 4567890127L, 8013579257L, 8888888891L, 7924680217L, 8901234581L, 2824657421L};

    // Actual sketch grid
    public int _x = 0;
    public int _y = 0;
    public long[][] _sketch;

    // the constructor takes the size dimensions and initialzes the sketch
    // storage.  we limit x to the size of maximum int, and y to 10 because
    // that's all the primes that are coded in the above table.  in theory you
    // could generate random primes on the fly.  that is expensive, and using
    // more than 10 hash functions also becomes very computation expensive
    // since ALL hash functions be calculated for every insertion and lookup.
    public SketchCountMin(int x, int y) {
	if(x>Integer.MAX_VALUE) {
	    x = Integer.MAX_VALUE;
	    System.out.println("new SketchCountMin: x reduced to " + Integer.MAX_VALUE);
	}
	if(y>10) {
	    y = 10;
	    System.out.println("new SketchCountMin: y reduced to 10");
	}
	_x = x;
	_y = y;
	_sketch = new long[_x][_y];
	System.out.println("Initialized new SketchCountMin[" + x + "][" + y + "]");
    }

    // helper function for converting result computed hash value down to the x
    // index into the array
    public int safeLongToInt(long l) {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
		throw new IllegalArgumentException
			(l + " cannot be cast to int without changing its value.");
	}
	int zz = (int) l;
	if(zz >= 0) {
		return zz;
	} else {
		int rv = (_x+zz);
//		System.out.println("ZKM: got " + zz + " returning " + rv);
		return rv;
	}
    }


    // this function computes the hashes for a given input.  it then adds delta
    // (which can be zero if you are just interested in reading the value) to
    // each bucket and returns the min of all buckets.

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
//	    System.out.println("C[" + x + "][" + y + "] was " + prev + " now " + _sketch[x][y]);

	    // is the new value the minimum so far?
            if ((min == -1) || (_sketch[x][y] < min)) {
                min = _sketch[x][y];
            }
        }
//        System.out.println("UPDATE SKETCH FOR " + k + " DELTA " + delta + " RESULT " + min);
        return min;
    }

    public void DumpSketch() {
	for(int y = 0; y < _y; y++) {
	    System.out.print("HASH " + y + " [ ");
	    for(int x = 0; x < _x; x++) {
		System.out.print(_sketch[x][y] + " ");
	    }
	    System.out.print("]\n");
	}
    }
}
