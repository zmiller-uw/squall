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

import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.visitors.OperatorVisitor;

public class SketchOperator extends OneToOneOperator implements Operator {
    private static final long serialVersionUID = 1L;

    private List<ValueExpression> _veList = new ArrayList<ValueExpression>();

	private int _numTuplesProcessed = 0;
    public int _field = -1;
    public int _x = -1;
    public int _y = -1;
    public int _s = -1;

    public SketchOperator(int field, int y, int x, int s) {
	System.out.println("ZKM: Instantiating SketchOperator " + field + " " + y + " " + x + " " + s);
	_field = field;
	_y = y;
	_x = x;
	_s = s;
    }

    @Override
    public void accept(OperatorVisitor ov) {
	ov.visit(this);
    }

    @Override
    public List<String> getContent() {
	throw new RuntimeException(
		"getContent for SketchOperator should never be invoked!");
    }

    public List<ValueExpression> getExpressions() {
	return _veList;
    }

    @Override
    public int getNumTuplesProcessed() {
	return _numTuplesProcessed;
    }

    @Override
    public boolean isBlocking() {
	return false;
    }

    @Override
    public String printContent() {
	throw new RuntimeException(
		"printContent for SketchOperator should never be invoked!");
    }

    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
	// break the tuple apart, sketch the column we're interested in
	System.out.println("ZKM: sketching field " + _field + " of tuple " + tuple);
	// change nothing
	return tuple;
    }

    @Override
    public String toString() {
	return "ZKM_toString";
    }
}
