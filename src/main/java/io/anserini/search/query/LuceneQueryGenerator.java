package io.anserini.search.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.QueryTermExtractor;
import org.apache.lucene.search.highlight.WeightedTerm;

import java.util.ArrayList;

public class LuceneQueryGenerator extends QueryGenerator {

    private QueryParser getQueryParser(String field, Analyzer analyzer, String queryText) {
        QueryParser parser = new QueryParser(field, analyzer);
        parser.setSplitOnWhitespace(true);
        parser.setAutoGeneratePhraseQueries(true);
        return parser;
    }

    @Override
    public Query buildQuery(String field, Analyzer analyzer, String queryText) {
        QueryParser parser = getQueryParser(field, analyzer, queryText);
        try {
            return parser.parse(queryText);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String escape(String field, Analyzer analyzer, String queryText) throws ParseException {
        QueryParser parser = getQueryParser(field, analyzer, queryText);
        Query query = parser.parse(queryText);
        WeightedTerm[] terms = QueryTermExtractor.getTerms(query);
        ArrayList<String> t = new ArrayList<>(terms.length);
        for (WeightedTerm term : terms) {
            t.add(term.getTerm());
        }
        return String.join(" ", t);
    }

}
