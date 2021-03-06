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

package ch.epfl.data.squall.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.storage.AggregationStorage;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.storage.WindowAggregationStorage;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.visitors.OperatorVisitor;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class HeavyHittersOperator extends OneToOneOperator implements AggregateOperator<Long> {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(HeavyHittersOperator.class);

    // the GroupBy type
    private static final int GB_UNSET = -1;
    private static final int GB_COLUMNS = 0;
    private static final int GB_PROJECTION = 1;

    private DistinctOperator _distinct;
    private int _groupByType = GB_UNSET;
    private List<Integer> _groupByColumns = new ArrayList<Integer>();
    private ProjectOperator _groupByProjection;
    private int _numTuplesProcessed = 0;

    private final NumericType<Long> _wrapper = new LongType();
    private BasicStore<Long> _storage;

    private final Map _map;
    
	private List<String> _heavyHitters = new ArrayList<String>();
    private Map<Object, Integer> _heavyHittersMap = new HashMap<Object, Integer>(); 
    private Random _random = new Random();
    private double _samplePercent = 1;
    
    private boolean isWindowSemantics;
    private int _windowRangeSecs = -1;
    private int _slideRangeSecs = -1;

    private int _field;

    public HeavyHittersOperator(int field, Map map, double samplePercent) {
		_field = field;
		_map = map;
		_samplePercent = samplePercent;
		_storage = new AggregationStorage<Long>(this, _wrapper, _map, true);
		System.out.println("[HeavyHittersOperator] initializing with samplePercent=" + samplePercent);
    }

    @Override
    public void accept(OperatorVisitor ov) {
    	ov.visit(this);
    }

    private boolean alreadySetOther(int GB_COLUMNS) {
    	return (_groupByType != GB_COLUMNS && _groupByType != GB_UNSET);
    }

    @Override
    public void clearStorage() {
    	_storage.reset();
    }

    // for this method it is essential that HASH_DELIMITER, which is used in
    // tupleToString method,
    // is the same as DIP_GLOBAL_ADD_DELIMITER
    @Override
    public List<String> getContent() {
		final String str = _storage.getContent();
		return str == null ? null : Arrays.asList(str.split("\\r?\\n"));
    }

    @Override
    public DistinctOperator getDistinct() {
    	return _distinct;
    }

    @Override
    public List<ValueExpression> getExpressions() {
    	return new ArrayList<ValueExpression>();
    }

    @Override
    public List<Integer> getGroupByColumns() {
    	return _groupByColumns;
    }

    @Override
    public ProjectOperator getGroupByProjection() {
	return _groupByProjection;
    }

    private String getGroupByStr() {
		final StringBuilder sb = new StringBuilder();
		sb.append("(");
		for (int i = 0; i < _groupByColumns.size(); i++) {
		    sb.append(_groupByColumns.get(i));
		    if (i == _groupByColumns.size() - 1)
			sb.append(")");
		    else
			sb.append(", ");
		}
		return sb.toString();
    }

    @Override
    public int getNumTuplesProcessed() {
    	return _numTuplesProcessed;
    }

    @Override
    public BasicStore getStorage() {
    	return _storage;
    }

    @Override
    public Type getType() {
    	return _wrapper;
    }

    @Override
    public boolean hasGroupBy() {
    	return _groupByType != GB_UNSET;
    }

    @Override
    public boolean isBlocking() {
    	return true;
    }

    @Override
    public String printContent() {
    	return _storage.getContent();
    }

    // from Operator
    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
    	
		_numTuplesProcessed++;
		//System.out.println("[HeavyHittersOpeartor.processOne] _numTuplesProcessed=" + _numTuplesProcessed + ", tuple=" + tuple.toString());
		
		for(int index = 0; index < tuple.size(); index ++)
		{
		
			String recordValue = tuple.get(index);
			
			
			// Check if this value already exists in the heavy hitters map
			if(_heavyHittersMap.containsKey(recordValue)) {
				_heavyHittersMap.put(recordValue, _heavyHittersMap.get(recordValue) + 1);
				//System.out.println("\t=> incrementing \"" + recordValue + "\", count=" + _heavyHittersMap.get(recordValue));
			}
			
			// If the item does not exist in the map, give it some chance to be added
			else if(_random.nextDouble() < _samplePercent) {
				//Enter into map with 1 hit
				_heavyHittersMap.put(recordValue, 1);
				//System.out.println("\t adding new word to map: \"" + recordValue + "\", count=" + _heavyHittersMap.get(recordValue));
			}
		}
			
			
			
			
		// After every X number of records, update the heavy hitters.
		if(_numTuplesProcessed % 10000 == 0) {
			
			// Sort the map of heavy hitters
			_heavyHittersMap = sortByComparator(_heavyHittersMap, false);
			
			// Initialize the list of top-5 heavy hitters
			_heavyHitters = new ArrayList<String>();
			
			// Determine the top N heavy hitters
			Iterator it = _heavyHittersMap.entrySet().iterator();
			int heavyHittersCount = 0;
			while(it.hasNext() && heavyHittersCount < 100) {
				Map.Entry pair = (Map.Entry)it.next();
				_heavyHitters.add(pair.getKey().toString() + "=" + pair.getValue().toString());
				heavyHittersCount++;
			}
			
			System.out.println("[HeavyHittersOpeartor.processOne] _numTuplesProcessed=" + _numTuplesProcessed + ", _heavyHittersMap.size=" + _heavyHittersMap.size());
			
			System.out.print("\t=> RETURNING: [");
			for(int i = 0; i < _heavyHitters.size(); i ++) {
				System.out.print(_heavyHitters.get(i).toString() + ",");
			}
			System.out.print("]\n\n");
		}
		
		
		return _heavyHitters;
				
		
    }

    // actual operator implementation
    @Override
    public Long runAggregateFunction(Long value, List<String> tuple) {
    	return value + 1;
    }

    @Override
    public Long runAggregateFunction(Long value1, Long value2) {
    	return value1 + value2;
    }

    @Override
    public HeavyHittersOperator setDistinct(DistinctOperator distinct) {
		_distinct = distinct;
		return this;
    }

    @Override
    public HeavyHittersOperator setGroupByColumns(int... hashIndexes) {
    	return setGroupByColumns(Arrays
    			.asList(ArrayUtils.toObject(hashIndexes)));
    }

    // from AgregateOperator
    @Override
    public HeavyHittersOperator setGroupByColumns(List<Integer> groupByColumns) {
		if (!alreadySetOther(GB_COLUMNS)) {
		    _groupByType = GB_COLUMNS;
		    _groupByColumns = groupByColumns;
		    _storage.setSingleEntry(false);
		    return this;
		} else
		    throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public HeavyHittersOperator setGroupByProjection(ProjectOperator groupByProjection) {
		if (!alreadySetOther(GB_PROJECTION)) {
		    _groupByType = GB_PROJECTION;
		    _groupByProjection = groupByProjection;
		    _storage.setSingleEntry(false);
		    return this;
		} else
		    throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("HeavyHittersOperator ");
		if (_groupByColumns.isEmpty() && _groupByProjection == null)
		    sb.append("\n  No groupBy!");
		else if (!_groupByColumns.isEmpty())
		    sb.append("\n  GroupByColumns are ").append(getGroupByStr())
			    .append(".");
		else if (_groupByProjection != null)
		    sb.append("\n  GroupByProjection is ")
			    .append(_groupByProjection.toString()).append(".");
		if (_distinct != null)
		    sb.append("\n  It also has distinct ").append(_distinct.toString());
		return sb.toString();
    }

    @Override
    public AggregateOperator<Long> SetWindowSemantics(int windowRangeInSeconds, int windowSlideInSeconds) {
		WindowSemanticsManager._IS_WINDOW_SEMANTICS = true;
		isWindowSemantics = true;
		_windowRangeSecs = windowRangeInSeconds;
		_slideRangeSecs = windowSlideInSeconds;
		_storage = new WindowAggregationStorage<>(this, _wrapper, _map, true,
			_windowRangeSecs, _slideRangeSecs);
		if (_groupByColumns != null || _groupByProjection != null)
		    _storage.setSingleEntry(false);
		return this;
    }

    @Override
    public AggregateOperator<Long> SetWindowSemantics(int windowRangeInSeconds) {
    	return SetWindowSemantics(windowRangeInSeconds, windowRangeInSeconds);
    }

    @Override
    public int[] getWindowSemanticsInfo() {
		int[] res = new int[2];
		res[0] = _windowRangeSecs;
		res[1] = _slideRangeSecs;
		return res;
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

    /*
	 * Sorts a Map structure.
	 * Stolen from: http://stackoverflow.com/questions/8119366/sorting-hashmap-by-values
	 */
	private static Map<Object, Integer> sortByComparator(Map<Object, Integer> _heavyHittersMap2, final boolean order)
    {

        List<Entry<Object, Integer>> list = new LinkedList<Entry<Object, Integer>>(_heavyHittersMap2.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<Object, Integer>>()
        {
            public int compare(Entry<Object, Integer> o1,
                    Entry<Object, Integer> o2)
            {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<Object, Integer> sortedMap = new LinkedHashMap<Object, Integer>();
        for (Entry<Object, Integer> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
}
