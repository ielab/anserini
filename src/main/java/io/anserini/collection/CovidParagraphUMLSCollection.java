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

package io.anserini.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A document collection for the CORD-19 dataset provided by Semantic Scholar.
 */
public class CovidParagraphUMLSCollection extends DocumentCollection<CovidParagraphUMLSCollection.Document> {
    private static final Logger LOG = LogManager.getLogger(CovidParagraphUMLSCollection.class);

    private String hasCovid;

    public CovidParagraphUMLSCollection(Path path) {
        this.path = path;
        this.allowedFileSuffix = Set.of(".csv");
    }

    @Override
    public FileSegment<CovidParagraphUMLSCollection.Document> createFileSegment(Path p) throws IOException {
        return new Segment(p);
    }

    /**
     * A file containing a single CSV document.
     */
    public class Segment extends FileSegment<CovidParagraphUMLSCollection.Document> {
        CSVParser csvParser = null;
        private CSVRecord record = null;
        private Iterator<CSVRecord> iterator = null; // iterator for CSV records
        private Iterator<JsonNode> abstractIterator = null; // iterator for paragraphs in a CSV record
        private Iterator<JsonNode> paragraphIterator = null; // iterator for paragraphs in a CSV record
        private Integer paragraphNumber = 0;
        private Integer abstractNumber = 0;

        public Segment(Path path) throws IOException {
            super(path);
            bufferedReader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(path.toString())));

            csvParser = new CSVParser(bufferedReader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim());

            iterator = csvParser.iterator();
        }

        @Override
        public void readNext() throws NoSuchElementException {
            if (abstractIterator != null && abstractIterator.hasNext()) {
                bufferedRecord = createSourceDoc(abstractIterator.next(), abstractNumber, "a");
                abstractNumber++;
            } else if (paragraphIterator != null && paragraphIterator.hasNext()) {
                bufferedRecord = createSourceDoc(paragraphIterator.next(), paragraphNumber, "c");
                paragraphNumber++;
            } else if (iterator.hasNext()) {
                // if CSV contains more lines, we parse the next record
                record = iterator.next();

                // get paragraphs from full text file
                String fullTextPath = null;
                if (record.get("has_pmc_xml_parse").contains("True")) {
                    fullTextPath = "/" + record.get("full_text_file") + "/pmc_json/" + record.get("pmcid") + ".xml.json";
                } else if (record.get("has_pdf_parse").contains("True")) {
                    String[] hashes = record.get("sha").split(";");
//                    fullTextPath = "/" + record.get("full_text_file") + "/pdf_json/" + hashes[0].strip() + ".json";
                    fullTextPath = "/" + record.get("full_text_file") + "/pdf_json/" + hashes[hashes.length - 1].strip() + ".json";
                } else if (record.get("has_pmc_xml_parse").contains("False") && record.get("has_pdf_parse").contains("False")) {
                    String path = "/newJsonFiles/" + record.get("cord_uid") + ".json";
                    File f = new File(path);
                    if (f.exists() && !f.isDirectory()) {
                        fullTextPath = path;
                    }
                }

                String titleCUIs = "";
                String titleSemtypes = "";
                String title = "";
                if (fullTextPath != null) {
                    try {
                        String recordFullTextPath = CovidParagraphUMLSCollection.this.path.toString() + fullTextPath;
                        FileReader recordFullTextFileReader = new FileReader(recordFullTextPath);
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode recordJsonNode = mapper.readerFor(JsonNode.class).readTree(recordFullTextFileReader);
                        if (recordJsonNode.has("abstract")) {
                            abstractIterator = recordJsonNode.get("abstract").elements();
                        }
                        paragraphIterator = recordJsonNode.get("body_text").elements();
                        if (recordJsonNode.has("metadata")) {
                            if (recordJsonNode.get("metadata").has("title_umls_semtypes")) {
                                titleSemtypes = recordJsonNode.get("metadata").get("title_umls_semtypes").asText();
                            }
                            if (recordJsonNode.get("metadata").has("title_umls_concepts")) {
                                titleCUIs = recordJsonNode.get("metadata").get("title_umls_concepts").asText();
                            }
                            if (recordJsonNode.get("metadata").has("title")) {
                                title = recordJsonNode.get("metadata").get("title").asText();
                            }
                        }
                        if (recordJsonNode.has("hasCovid19")) {
                            hasCovid = recordJsonNode.get("hasCovid19").asText();
                        } else {
                            hasCovid = "False";
                        }
                    } catch (IOException e) {
                        LOG.error("Error parsing file at " + fullTextPath + "\n" + e.getMessage());
                    }
                } else {
                    abstractIterator = null;
                    paragraphIterator = null;
                }

                bufferedRecord = new CovidParagraphUMLSCollection.Document(record, title, titleCUIs, titleSemtypes, hasCovid, 0, "t");
                paragraphNumber = 0;
                abstractNumber = 0;
            } else {
                throw new NoSuchElementException("Reached end of CSVRecord Entries Iterator");
            }
        }

        private Document createSourceDoc(JsonNode node, Integer paragraphNumber, String paragraphType) {
            // if the record contains more paragraphs, we parse them
            String paragraph = node.get("text").asText();
            String umls = "";
            String semtypes = "";
            String umlsStartAttr;
            switch (paragraphType) {
                case "a":
                case "c":
                    umlsStartAttr = "text_";
                    break;
                default:
                    umlsStartAttr = "title_";
            }
            String umlsAttr = umlsStartAttr + "umls_concepts";
            String semtypesAttr = umlsStartAttr + "umls_semtypes";
            if (node.has(umlsAttr)) {
                umls = node.get(umlsAttr).asText();
            }
            if (node.has(semtypesAttr)) {
                semtypes = node.get(semtypesAttr).asText();
            }
            return new Document(record, paragraph, umls, semtypes, hasCovid, paragraphNumber, paragraphType);
        }

        @Override
        public void close() {
            super.close();
            if (csvParser != null) {
                try {
                    csvParser.close();
                } catch (IOException e) {
                    // do nothing
                }
            }
        }
    }

    /**
     * A document in a CORD-19 collection.
     */
    public class Document extends CovidUMLSCollectionDocument {
        public Document(CSVRecord record, String paragraph, String cuis, String semtypes, String hasCovid, Integer paragraphNumber, String paragraphType) {
            this.fields = new HashMap<>();

            this.record = record;
            this.fields.put("umls", cuis);
            this.fields.put("semtypes", semtypes);
            this.fields.put("has_covid", hasCovid);
            this.id = record.get("cord_uid") + "_" + paragraphType + "_" + String.format("%05d", paragraphNumber);
            this.content = paragraph;
        }
    }
}
