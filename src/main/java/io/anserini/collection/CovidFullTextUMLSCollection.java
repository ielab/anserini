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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A document collection for the CORD-19 dataset provided by Semantic Scholar.
 */
public class CovidFullTextUMLSCollection extends DocumentCollection<CovidFullTextUMLSCollection.Document> {
    private static final Logger LOG = LogManager.getLogger(CovidFullTextUMLSCollection.class);

    public CovidFullTextUMLSCollection(Path path) {
        this.path = path;
        this.allowedFileSuffix = Set.of(".csv");
    }

    @Override
    public FileSegment<CovidFullTextUMLSCollection.Document> createFileSegment(Path p) throws IOException {
        return new Segment(p);
    }

    /**
     * A file containing a single CSV document.
     */
    public class Segment extends FileSegment<CovidFullTextUMLSCollection.Document> {
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
            if (iterator.hasNext()) {
                // if CSV contains more lines, we parse the next record
                record = iterator.next();

                // get paragraphs from full text file
                String fullTextPath = null;
                if (record.get("has_pmc_xml_parse").contains("True")) {
                    fullTextPath = "/" + record.get("full_text_file") + "/pmc_json/" + record.get("pmcid") + ".xml.json";
                } else if (record.get("has_pdf_parse").contains("True")) {
                    String[] hashes = record.get("sha").split(";");
                    fullTextPath = "/" + record.get("full_text_file") + "/pdf_json/" + hashes[0].strip() + ".json";
                } else if (record.get("has_pmc_xml_parse").contains("False") && record.get("has_pdf_parse").contains("False")) {
                    fullTextPath = "/" + record.get("full_text_file") + "/" + record.get("cord_id") + ".json";
                }

                String content = "";
                String umls = "";
                String semtypes = "";
                String hasCovid = "false";
                String fullText = "";
                if (fullTextPath != null) {
                    try {
                        String recordFullTextPath = CovidFullTextUMLSCollection.this.path.toString() + fullTextPath;
                        FileReader recordFullTextFileReader = new FileReader(recordFullTextPath);
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode recordJsonNode = mapper.readerFor(JsonNode.class).readTree(recordFullTextFileReader);
                        if (recordJsonNode.has("abstract")) {
                            abstractIterator = recordJsonNode.get("abstract").elements();
                        }
                        paragraphIterator = recordJsonNode.get("body_text").elements();
                        if (recordJsonNode.has("metadata")) {
                            if (recordJsonNode.get("metadata").has("umls")) {
                                umls = recordJsonNode.get("metadata").get("umls").asText();
                            }
                            if (recordJsonNode.get("metadata").has("semtypes")) {
                                semtypes = recordJsonNode.get("metadata").get("semtypes").asText();
                            }
                            if (recordJsonNode.get("metadata").has("title")) {
                                content = recordJsonNode.get("metadata").get("title").asText();
                            }
                            if (recordJsonNode.has("isCovidI9")) {
                                hasCovid = recordJsonNode.get("isCovidI9").asText();
                            }
                        }

                        if (abstractIterator != null && abstractIterator.hasNext()) {
                            JsonNode node = abstractIterator.next();
                            content += node.get("text").asText();
                            fullText += node.get("text").asText();
                            if (node.has("umls")) {
                                umls = String.join(",", umls, node.get("umls").asText());
                                semtypes = String.join(",", semtypes, node.get("umls").asText());
                            }
                        } else if (paragraphIterator != null && paragraphIterator.hasNext()) {
                            JsonNode node = paragraphIterator.next();
                            content += node.get("text").asText();
                            fullText += node.get("text").asText();
                            if (node.has("umls")) {
                                umls = String.join(",", umls, node.get("umls").asText());
                                semtypes = String.join(",", semtypes, node.get("umls").asText());
                            }
                        }

                    } catch (IOException e) {
                        LOG.error("Error parsing file at " + fullTextPath + "\n" + e.getMessage());
                    }
                } else {
                    abstractIterator = null;
                    paragraphIterator = null;
                }

                bufferedRecord = new CovidFullTextUMLSCollection.Document(record, content, umls, semtypes, hasCovid);
                bufferedRecord.fields.put("full_text", fullText);
                paragraphNumber = 0;
                abstractNumber = 0;
            } else {
                throw new NoSuchElementException("Reached end of CSVRecord Entries Iterator");
            }
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
        public Document(CSVRecord record, String paragraph, String cuis, String semtypes, String hasCovid) {
            this.fields = new HashMap<>();

            this.record = record;
            this.fields.put("umls", cuis);
            this.fields.put("semtypes", semtypes);
            this.fields.put("has_covid", hasCovid);
            this.id = record.get("cord_uid");
            this.content = paragraph;
        }
    }
}
