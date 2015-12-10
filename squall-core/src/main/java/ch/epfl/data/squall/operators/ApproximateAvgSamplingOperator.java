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
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.storage.AggregationStorage;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.storage.WindowAggregationStorage;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.SumCount;
import ch.epfl.data.squall.types.SumCountType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.visitors.OperatorVisitor;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class ApproximateAvgSamplingOperator extends OneToOneOperator implements AggregateOperator<SumCount> {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(ApproximateAvgSamplingOperator.class);

    // the GroupBy type
    private static final int GB_UNSET = -1;
    private static final int GB_COLUMNS = 0;
    private static final int GB_PROJECTION = 1;

    private DistinctOperator _distinct;
    private int _groupByType = GB_UNSET;
    private List<Integer> _groupByColumns = new ArrayList<Integer>();
    private ProjectOperator _groupByProjection;
    private int _numTuplesProcessed = 0;

    private final SumCountType _wrapper = new SumCountType();
    private final ValueExpression _ve;
    private BasicStore<SumCount> _storage;

    private final Map _map;
	private final int _popSize;

    private boolean isWindowSemantics;
    private int _windowRangeSecs = -1;
    private int _slideRangeSecs = -1;

    public ApproximateAvgSamplingOperator(ValueExpression ve, Map map, int popSize) {
	_ve = ve;
	_map = map;
	_popSize = popSize;
	_storage = new AggregationStorage<SumCount>(this, _wrapper, _map, true);
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

    @Override
    public List<String> getContent() {
	throw new UnsupportedOperationException(
		"getContent for ApproximateAvgSamplingOperator is not supported yet.");
    }

    @Override
    public DistinctOperator getDistinct() {
	return _distinct;
    }

    @Override
    public List<ValueExpression> getExpressions() {
	final List<ValueExpression> result = new ArrayList<ValueExpression>();
	result.add(_ve);
	return result;
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
		String storageContent = _storage.getContent();
		System.out.println("StorageContent: ");
		System.out.println(storageContent);
		
		String lines[] = storageContent.split("\\r?\\n");
		double currentValue = 0;
		int currentCount = 0;
		double pointEstimateAvg = 0, popLower = 0, popUpper = 0;
		double pSample = 0, pLower = 0, pUpper = 0;
		double stdError = 0;
		double zScore = 1.96;
		String resultString = "Number of Tuples Processed: " + _numTuplesProcessed + "\n";
		for (String line: lines) {
			String fields[] = line.split(" = ");
			String subfields[] = fields[1].split(":");
			currentValue = Double.parseDouble(subfields[0]);
			currentCount = Integer.parseInt(subfields[1]);
			pointEstimateAvg = 1.0*currentValue/currentCount;
			System.out.println("currentValue: " + currentValue + " | currentCount: " + currentCount + " | pointEstimateAvg: " + pointEstimateAvg);
			
			pSample = 1.0*currentCount/_numTuplesProcessed;
			resultString += fields[0];
			resultString += " = " + pointEstimateAvg;	
			
			if (_numTuplesProcessed*pSample <= 10.0) {
				resultString += " | Too early for Interval Estimate.\n";
			}
			else {
				stdError = Math.sqrt(pSample*(1-pSample)/_numTuplesProcessed);
				pLower = pSample - (zScore*stdError);
				pUpper = pSample + (zScore*stdError);
				popLower = (int)Math.round(pLower*_popSize);
				popUpper = (int)Math.round(pUpper*_popSize);
				// System.out.print(" | pSample: " + pSample + " | pLower: " + pLower + " | pUpper: " + pUpper);
				resultString += " [ " + popLower + " , " + popUpper + " ] | 95% Confidence Interval Range = " + (popUpper - popLower);
			}
			resultString += " | CV: " + currentValue + "\n";
			
		}
		return resultString;
		// return _storage.getContent();
	}

    // from Operator
    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
	_numTuplesProcessed++;
	if (_distinct != null) {
	    tuple = _distinct.processOne(tuple, lineageTimestamp);
	    if (tuple == null)
		return null;
	}
	String tupleHash;
	if (_groupByType == GB_PROJECTION)
	    tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,  _groupByProjection.getExpressions(), _map);
	else
	    tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,  _map);
	final SumCount sumCount = _storage.update(tuple, tupleHash);
	final String strValue = _wrapper.toString(sumCount);

	// propagate further the affected tupleHash-tupleValue pair
	final List<String> affectedTuple = new ArrayList<String>();
	affectedTuple.add(tupleHash);
	affectedTuple.add(strValue);
	
	if (_numTuplesProcessed%100 ==0) {
		System.out.println("TupleHash: " + tupleHash + " | Tuple: " + tuple + " | LTS: " + lineageTimestamp + " | StrValue: " + strValue); 
		System.out.println("SumCount: " + sumCount); 
		System.out.println("Number of Tuples Processed = " + _numTuplesProcessed);
		System.out.println(printContent());
	}
		
	return affectedTuple;
    }

    // actual operator implementation
    @Override
    public SumCount runAggregateFunction(SumCount value, List<String> tuple) {
	Double sumDelta;
	Long countDelta;

	final Type veType = _ve.getType();
	if (veType instanceof SumCountType) {
	    // when merging results from multiple Components which have SumCount
	    // as the output
	    final SumCount sc = (SumCount) _ve.eval(tuple);
	    sumDelta = sc.getSum();
	    countDelta = sc.getCount();
	} else {
	    final NumericType nc = (NumericType) veType;
	    sumDelta = nc.toDouble(_ve.eval(tuple));
	    countDelta = 1L;
	}

	final Double sumNew = sumDelta + value.getSum();
	final Long countNew = countDelta + value.getCount();

	return new SumCount(sumNew, countNew);
    }

    @Override
    public SumCount runAggregateFunction(SumCount value1, SumCount value2) {
		final Double sumNew = value1.getSum() + value2.getSum();
		final Long countNew = value1.getCount() + value2.getCount();
		return new SumCount(sumNew, countNew);
    }

    @Override
    public ApproximateAvgSamplingOperator setDistinct(DistinctOperator distinct) {
	_distinct = distinct;
	return this;
    }

    @Override
    public ApproximateAvgSamplingOperator setGroupByColumns(int... hashIndexes) {
	return setGroupByColumns(Arrays
		.asList(ArrayUtils.toObject(hashIndexes)));
    }

    // from AgregateOperator
    @Override
    public ApproximateAvgSamplingOperator setGroupByColumns(List<Integer> groupByColumns) {
	if (!alreadySetOther(GB_COLUMNS)) {
	    _groupByType = GB_COLUMNS;
	    _groupByColumns = groupByColumns;
	    _storage.setSingleEntry(false);
	    return this;
	} else
	    throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public ApproximateAvgSamplingOperator setGroupByProjection(
	    ProjectOperator groupByProjection) {
	if (!alreadySetOther(GB_PROJECTION)) {
	    _groupByType = GB_PROJECTION;
	    _groupByProjection = groupByProjection;
	    _storage.setSingleEntry(false);
	    return this;
	} else
	    throw new RuntimeException("Aggregation already has groupBy set!");
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	sb.append("ApproximateAvgSamplingOperator with VE: ");
	sb.append(_ve.toString());
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
    public AggregateOperator<SumCount> SetWindowSemantics(
	    int windowRangeInSeconds, int windowSlideInSeconds) {
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
    public AggregateOperator<SumCount> SetWindowSemantics(
	    int windowRangeInSeconds) {
	return SetWindowSemantics(windowRangeInSeconds, windowRangeInSeconds);

    }

    @Override
    public int[] getWindowSemanticsInfo() {
	int[] res = new int[2];
	res[0] = _windowRangeSecs;
	res[1] = _slideRangeSecs;
	return res;
    }

}

/*
double std_dev(double a[], int n) {
    if(n == 0)
    	return 0.0;
    int i = 0;
    double meanSum = a[0];
    double stdDevSum = 0.0;
    for(i = 1; i < n; ++i) {
        double stepSum = a[i] - meanSum;
        double stepMean = ((i - 1) * stepSum) / i;
        meanSum += stepMean;
        stdDevSum += stepMean * stepSum;
    }
    // for poulation variance: return sqrt(stdDevSum / elements);
    return sqrt(stdDevSum / (elements - 1));
}

double std_dev(double newValue) {
    // assume n is maintained inside
	// assume meanSum is maintained inside
	// assume stdDevSum is maintained inside
	
	n++;
	if(n == 1)	return 0.0;
	double stepSum = newValue - meanSum;
	double stepMean = ((n - 1) * stepSum) / n;
	meanSum += stepMean;
    stdDevSum += stepMean * stepSum;
    return sqrt(stdDevSum / (n - 1));
}

*/
