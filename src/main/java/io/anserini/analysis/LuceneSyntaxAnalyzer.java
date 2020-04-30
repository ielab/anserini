package io.anserini.analysis;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class LuceneSyntaxAnalyzer extends StopwordAnalyzerBase {


    public LuceneSyntaxAnalyzer() {
        super(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        StandardTokenizer src = new StandardTokenizer();
        TokenStream result;
        result = src;
        result = new EnglishPossessiveFilter(result);
        result = new LowerCaseFilter(result);
        result = new StopFilter(result, this.stopwords);
        return new TokenStreamComponents(src, result);
    }
}
