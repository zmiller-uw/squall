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

package ch.epfl.data.squall.examples.imperative.shj;

import java.util.*;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.operators.HeavyHittersOperator;
import ch.epfl.data.squall.operators.TwitterParserOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.IntegerType;

public class HeavyHittersPlan extends QueryPlan {

    public HeavyHittersPlan(String dataPath, String extension, Map conf) {
        super(dataPath, extension, conf);
    }

    @Override
    public Component createQueryPlan(String dataPath, String extension, Map conf) {
    
    	double samplePercent = 1;
    	if(conf.containsKey("SAMPLE_PERCENT")) {
    		samplePercent = Double.parseDouble(conf.get("SAMPLE_PERCENT").toString());
    	}

        // -------------------------------------------------------------------------------------
        Component tweets = new DataSourceComponent("tweets", conf)
                .add(new ProjectOperator(1));

     	// -------------------------------------------------------------------------------------
        Component tweetsHeavyHitterKeywords = tweets
				.add(new TwitterParserOperator(0, conf).setGroupByColumns(0))
				.add(new HeavyHittersOperator(0, conf, samplePercent));

        return tweetsHeavyHitterKeywords;
        // -------------------------------------------------------------------------------------
    }
}

