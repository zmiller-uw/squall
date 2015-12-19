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

public class TwitterParserOperator extends OneToOneOperator implements AggregateOperator<Long> {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(TwitterParserOperator.class);

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
    
    private boolean isWindowSemantics;
    private int _windowRangeSecs = -1;
    private int _slideRangeSecs = -1;

    private int _field;

    public TwitterParserOperator(int field, Map map) {
		_field = field;
		_map = map;
		_storage = new AggregationStorage<Long>(this, _wrapper, _map, true);
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
		//System.out.println("[TwitterParseOperator.processOne] tuple=" + tuple);
		
		// Setup variables
		List<String> stopWords = Arrays.asList("a", "able", "about", "above", "abst", "accordance", "according", "accordingly", "across", "act", "actually", "added", "adj", "affected", "affecting", "affects", "after", "afterwards", "again", "against", "ah", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "announce", "another", "any", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere", "apparently", "approximately", "are", "aren", "arent", "arise", "around", "as", "aside", "ask", "asking", "at", "auth", "available", "away", "awfully", "b", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "begin", "beginning", "beginnings", "begins", "behind", "being", "believe", "below", "beside", "besides", "between", "beyond", "biol", "both", "brief", "briefly", "but", "by", "c", "ca", "came", "can", "cannot", "can't", "cause", "causes", "certain", "certainly", "co", "com", "come", "comes", "contain", "containing", "contains", "could", "couldnt", "d", "date", "did", "didn't", "different", "do", "does", "doesn't", "doing", "done", "don't", "down", "downwards", "due", "during", "e", "each", "ed", "edu", "effect", "eg", "eight", "eighty", "either", "else", "elsewhere", "end", "ending", "enough", "especially", "et", "et-al", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "except", "f", "far", "few", "ff", "fifth", "first", "five", "fix", "followed", "following", "follows", "for", "former", "formerly", "forth", "found", "four", "from", "further", "furthermore", "g", "gave", "get", "gets", "getting", "give", "given", "gives", "giving", "go", "goes", "gone", "got", "gotten", "h", "had", "happens", "hardly", "has", "hasn't", "have", "haven't", "having", "he", "hed", "hence", "her", "here", "hereafter", "hereby", "herein", "heres", "hereupon", "hers", "herself", "hes", "hi", "hid", "him", "himself", "his", "hither", "home", "how", "howbeit", "however", "hundred", "i", "id", "ie", "if", "i'll", "i'm", "immediate", "immediately", "importance", "important", "in", "inc", "indeed", "index", "information", "instead", "into", "invention", "inward", "is", "isn't", "it", "itd", "it'd", "it'll", "its", "it's", "itself", "i've", "j", "just", "k", "keep     keeps", "kept", "kg", "km", "know", "known", "knows", "l", "largely", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "line", "little", "'ll", "look", "looking", "looks", "ltd", "m", "made", "mainly", "make", "makes", "many", "may", "maybe", "me", "mean", "means", "meantime", "meanwhile", "merely", "mg", "might", "million", "miss", "ml", "more", "moreover", "most", "mostly", "mr", "mrs", "much", "mug", "must", "my", "myself", "n", "na", "name", "namely", "nay", "nd", "near", "nearly", "necessarily", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "ninety", "no", "nobody", "non", "none", "nonetheless", "noone", "nor", "normally", "nos", "not", "noted", "nothing", "now", "nowhere", "o", "obtain", "obtained", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "omitted", "on", "once", "one", "ones", "only", "onto", "or", "ord", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "owing", "own", "p", "page", "pages", "part", "particular", "particularly", "past", "per", "perhaps", "placed", "please", "plus", "poorly", "possible", "possibly", "potentially", "pp", "predominantly", "present", "previously", "primarily", "probably", "promptly", "proud", "provides", "put", "q", "que", "quickly", "quite", "qv", "r", "ran", "rather", "rd", "re", "readily", "really", "recent", "recently", "ref", "refs", "regarding", "regardless", "regards", "related", "relatively", "research", "respectively", "resulted", "resulting", "results", "right", "run", "s", "said", "same", "saw", "say", "saying", "says", "sec", "section", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sent", "seven", "several", "shall", "she", "shed", "she'll", "shes", "should", "shouldn't", "show", "showed", "shown", "showns", "shows", "significant", "significantly", "similar", "similarly", "since", "six", "slightly", "so", "some", "somebody", "somehow", "someone", "somethan", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specifically", "specified", "specify", "specifying", "still", "stop", "strongly", "sub", "substantially", "successfully", "such", "sufficiently", "suggest", "sup", "sure     t", "take", "taken", "taking", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "that'll", "thats", "that've", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "thered", "therefore", "therein", "there'll", "thereof", "therere", "theres", "thereto", "thereupon", "there've", "these", "they", "theyd", "they'll", "theyre", "they've", "think", "this", "those", "thou", "though", "thoughh", "thousand", "throug", "through", "throughout", "thru", "thus", "til", "tip", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "ts", "twice", "two", "u", "un", "under", "unfortunately", "unless", "unlike", "unlikely", "until", "unto", "up", "upon", "ups", "us", "use", "used", "useful", "usefully", "usefulness", "uses", "using", "usually", "v", "value", "various", "'ve", "very", "via", "viz", "vol", "vols", "vs", "w", "want", "wants", "was", "wasnt", "way", "we", "wed", "welcome", "we'll", "went", "were", "werent", "we've", "what", "whatever", "what'll", "whats", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "wheres", "whereupon", "wherever", "whether", "which", "while", "whim", "whither", "who", "whod", "whoever", "whole", "who'll", "whom", "whomever", "whos", "whose", "why", "widely", "will", "willing", "wish", "with", "within", "without", "wont", "words", "world", "would", "wouldnt", "www", "x", "y", "yes", "yet", "you", "youd", "you'll", "your", "youre", "yours", "yourself", "yourselves", "you've", "z", "zero", "t", "http", "https", "rt");
		List<String> returnWords = new ArrayList<String>();
		String[] tweetWords;
		String thisTweetText;
		String tupleData = tuple.get(0).toString();
		
		// Extract useful words from the tweet
		thisTweetText = tupleData.toLowerCase().replaceAll("(\\r|\\n|)", "");
		tweetWords = thisTweetText.split(" ");
		
		// Return all useful words
		for(String thisWord : tweetWords) {
			
			// Clean up the word using a regular expression
			Pattern wordCleanPattern = Pattern.compile("^[\"',.?!;:()]*([a-z]([a-z'\\-]*[a-z])?)[\"',.?!;:()]*$");
			Matcher wordCleanMatcher = wordCleanPattern.matcher(thisWord);
			String cleanedWord = thisWord;
			
			// If the word checks out, maybe add it to the heavy hitters list
			if(wordCleanMatcher.matches() && !stopWords.contains(cleanedWord)) {
				cleanedWord = wordCleanMatcher.group(1).toString();
				//System.out.println("\t=> adding \"" + cleanedWord + "\"");
				returnWords.add(cleanedWord);
			}
		}
		
		return returnWords;
				
		
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
    public TwitterParserOperator setDistinct(DistinctOperator distinct) {
		_distinct = distinct;
		return this;
    }

    @Override
    public TwitterParserOperator setGroupByColumns(int... hashIndexes) {
    	return setGroupByColumns(Arrays
    			.asList(ArrayUtils.toObject(hashIndexes)));
    }

    // from AgregateOperator
    @Override
    public TwitterParserOperator setGroupByColumns(List<Integer> groupByColumns) {
		if (!alreadySetOther(GB_COLUMNS)) {
		    _groupByType = GB_COLUMNS;
		    _groupByColumns = groupByColumns;
		    _storage.setSingleEntry(false);
		    return this;
		} else
		    throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public TwitterParserOperator setGroupByProjection(ProjectOperator groupByProjection) {
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
