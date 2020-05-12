/*
 * Anserini: A Lucene toolkit for replicable information retrieval research
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

package io.anserini.index.generator;

import com.fasterxml.jackson.databind.JsonNode;
import io.anserini.analysis.DefaultEnglishAnalyzer;
import io.anserini.collection.CovidCollectionDocument;
import io.anserini.collection.CovidTrialstreamerCollection;
import io.anserini.collection.CovidUMLSCollectionDocument;
import io.anserini.index.IndexArgs;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

import java.io.StringReader;
import java.util.Map;

/**
 * Converts a {@link CovidCollectionDocument} into a Lucene {@link Document}, ready to be indexed.
 */
public class CovidUMLSGenerator implements LuceneDocumentGenerator<CovidUMLSCollectionDocument> {
    private IndexArgs args;

    public enum CovidField {
        SHA("sha"),
        SOURCE("source_x"),
        DOI("doi"),
        TITLE("title"),
        AUTHORS("authors"),
        AUTHOR_STRING("author_string"),
        ABSTRACT("abstract"),
        JOURNAL("journal"),
        PUBLISH_TIME("publish_time"),
        YEAR("year"),
        PMC_ID("pmcid"),
        PUBMED_ID("pubmed_id"),
        LICENSE("license"),
        MICROSOFT_ID("Microsoft Academic Paper ID"),
        WHO("WHO #Covidence"),
        URL("url"),
        UMLS("umls"),
        SEMTYPES("semtypes"),
        HAS_COVID("has_covid"),
        FULL_TEXT("full_text");

        public final String name;

        CovidField(String s) {
            name = s;
        }
    }

    public enum TrialstreamerField {
        OUTCOMES_VOCAB("outcomes_vocab"),
        POPULATION_VOCAB("population_vocab"),
        INTERVENTIONS_VOCAB("interventions_vocab");

        public final String name;

        TrialstreamerField(String s) {
            name = s;
        }
    }

    public CovidUMLSGenerator(IndexArgs args) {
        this.args = args;
    }

    @Override
    public Document createDocument(CovidUMLSCollectionDocument covidDoc) throws GeneratorException {
        String id = covidDoc.id();
        String content = covidDoc.contents();
        String raw = covidDoc.raw();
        Map<String, String> fields = covidDoc.getFields();

        if (content == null || content.trim().isEmpty()) {
            throw new EmptyDocumentException();
        }

        Document doc = new Document();

        // Store the collection docid.
        doc.add(new StringField(IndexArgs.ID, id, Field.Store.YES));
        // This is needed to break score ties by docid.
        doc.add(new SortedDocValuesField(IndexArgs.ID, new BytesRef(id)));

        if (args.storeRaw) {
            doc.add(new StoredField(IndexArgs.RAW, raw));
        }

        FieldType fieldType = new FieldType();
        fieldType.setStored(args.storeContents);

        // Are we storing document vectors?
        if (args.storeDocvectors) {
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorPositions(true);
        }

        // Are we building a "positional" or "count" index?
        if (args.storePositions) {
            fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        } else {
            fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        }

        // normal fields
        doc.add(new Field(IndexArgs.CONTENTS, content, fieldType));
        doc.add(new Field(CovidField.TITLE.name, covidDoc.record().get(CovidField.TITLE.name), fieldType));
        doc.add(new Field(CovidField.ABSTRACT.name, covidDoc.record().get(CovidField.ABSTRACT.name), fieldType));
        if (fields.containsKey("full_text")) {
            doc.add(new Field(CovidField.FULL_TEXT.name, fields.get("full_text"), fieldType));
        }
        if (fields.containsKey("umls")) {
            addUMLS(doc, fields.get("umls"), fieldType);
        }
        if (fields.containsKey("semtypes")) {
            addSemtypes(doc, fields.get("semtypes"), fieldType);
        }
        if (fields.containsKey("has_covid")) {
            doc.add(new Field(CovidField.HAS_COVID.name, fields.get("has_covid"), fieldType));
        }


        // string fields
        doc.add(new StringField(CovidField.SHA.name, covidDoc.record().get(CovidField.SHA.name), Field.Store.YES));
        doc.add(new StringField(CovidField.DOI.name, covidDoc.record().get(CovidField.DOI.name), Field.Store.YES));
        doc.add(new StringField(CovidField.SOURCE.name, covidDoc.record().get(CovidField.SOURCE.name), Field.Store.YES));
        doc.add(new StringField(CovidField.JOURNAL.name, covidDoc.record().get(CovidField.JOURNAL.name), Field.Store.YES));
        doc.add(new StringField(CovidField.WHO.name, covidDoc.record().get(CovidField.WHO.name), Field.Store.YES));
        doc.add(new StringField(CovidField.PMC_ID.name, covidDoc.record().get(CovidField.PMC_ID.name), Field.Store.YES));
        doc.add(new StringField(CovidField.PUBMED_ID.name,
                covidDoc.record().get(CovidField.PUBMED_ID.name), Field.Store.YES));
        doc.add(new StringField(CovidField.MICROSOFT_ID.name,
                covidDoc.record().get(CovidField.MICROSOFT_ID.name), Field.Store.YES));
        doc.add(new StringField(CovidField.PUBLISH_TIME.name,
                covidDoc.record().get(CovidField.PUBLISH_TIME.name), Field.Store.YES));
        doc.add(new StringField(CovidField.LICENSE.name,
                covidDoc.record().get(CovidField.LICENSE.name), Field.Store.YES));
        doc.add(new StringField(CovidField.URL.name,
                covidDoc.record().get(CovidField.URL.name), Field.Store.YES));

        // non-stemmed fields
        addAuthors(doc, covidDoc.record().get(CovidField.AUTHORS.name), fieldType);

        // parse year published
        try {
            doc.add(new IntPoint(CovidField.YEAR.name, Integer.parseInt(
                    covidDoc.record().get(CovidField.PUBLISH_TIME.name).strip().substring(0, 4))));
        } catch (Exception e) {
            // can't parse year
        }
        return doc;
    }

    private void addUMLS(Document doc, String umlsString, FieldType fieldType) {
        for (String umls : umlsString.split(",")) {
            if (umls.length() > 0) {
                addNonStemmedField(doc, CovidField.UMLS.name, umls, fieldType);
            }
        }
    }

    private void addSemtypes(Document doc, String semtypeString, FieldType fieldType) {
        for (String semtype : semtypeString.split(",")) {
            if (semtype.length() > 0) {
                addNonStemmedField(doc, CovidField.SEMTYPES.name, semtype, fieldType);
            }
        }
    }

    private void addAuthors(Document doc, String authorString, FieldType fieldType) {
        if (authorString == null || authorString == "") {
            return;
        }

        // index raw author string
        addNonStemmedField(doc, CovidField.AUTHOR_STRING.name, authorString, fieldType);

        // process all individual author names
        for (String author : authorString.split(";")) {
            addNonStemmedField(doc, CovidField.AUTHORS.name, processAuthor(author), fieldType);
        }
    }

    // process author name into a standard order if it is reversed and comma separated
    // eg) Jones, Bob -> Bob Jones
    private String processAuthor(String author) {
        String processedName = "";
        String[] splitNames = author.split(",");
        for (int i = splitNames.length - 1; i >= 0; --i) {
            processedName += splitNames[i].strip() + " ";
        }
        return processedName.strip();
    }

    // index field without stemming but store original string value
    private void addNonStemmedField(Document doc, String key, String value, FieldType fieldType) {
        FieldType nonStemmedType = new FieldType(fieldType);
        nonStemmedType.setStored(true);

        // token stream to be indexed
        Analyzer nonStemmingAnalyzer = DefaultEnglishAnalyzer.newNonStemmingInstance(CharArraySet.EMPTY_SET);
        TokenStream stream = nonStemmingAnalyzer.tokenStream(null, new StringReader(value));
        Field field = new Field(key, value, nonStemmedType);
        field.setTokenStream(stream);
        doc.add(field);
        nonStemmingAnalyzer.close();
    }
}
