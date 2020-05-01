package io.anserini.collection;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;

public class TrecClinicalMedicalCollection extends DocumentCollection<TrecClinicalMedicalCollection.Document> {

    private static final Logger LOG = LogManager.getLogger(TrecClinicalMedicalCollection.class);

    public TrecClinicalMedicalCollection(Path path) {
        this.path = path;
    }

    @Override
    public FileSegment<TrecClinicalMedicalCollection.Document> createFileSegment(Path p) throws IOException {
        return new Segment(p);
    }

    /**
     * A file in a TREC CDS collection.
     */
    public static class Segment extends FileSegment<TrecClinicalMedicalCollection.Document> {


        private boolean docDone = false;
        private Iterator<Path> iterator = null;

        public Segment(Path path) throws IOException {
            super(path);
            this.docDone = false;
        }

        @Override
        protected void readNext() throws IOException, ParseException, NoSuchElementException {
            if (!docDone) {
                bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toFile()), StandardCharsets.UTF_8));
                bufferedRecord = new TrecClinicalMedicalCollection.Document(bufferedReader, path.toString());
                docDone = true;
            } else {
                throw new NoSuchElementException("Reached end of TREC clinical entries");
            }
        }


    }

    public static class Document extends MultifieldSourceDocument {

        private String id;
        private String contents;
        private Map<String, String> fields;


        public Document(BufferedReader bRdr, String fileName) throws IOException {
            LOG.info("Creating a new TREC CDS document for "+fileName);

            ClinicalXMLDocumentHandler handler = new ClinicalXMLDocumentHandler();
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setValidating(false);
            try {
                factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                SAXParser saxParser = factory.newSAXParser();
                saxParser.parse(fileName, handler);
            } catch (ParserConfigurationException e) {
                throw new RuntimeException("Unable to parse the XML document" + fileName);
            } catch (SAXException se) {
                throw new RuntimeException(se.getMessage());
            } catch (Exception ee) {
                ee.printStackTrace();
            }

            fields = handler.getArticleContents();
            contents = StringUtils.join(fields, " ");

            if(fields.containsKey("article-id-pmid")) {
                id = fields.get("article-id-pmid");
            } else if(fields.containsKey("article-id-pmc")) {
                id = fields.get("article-id-pmc");
            } else if(fields.containsKey("article-id-publisher-id")) {
                id = fields.get("article-id-publisher-id");
            } else if(fields.containsKey("article-id-doi")) {
                id = fields.get("article-id-doi");
            } else {
                id = Integer.toString(fields.get("article-title").hashCode());
            }

        }



        @Override
        public String id() {
            return id;
        }

        @Override
        public String contents() {
            if (contents == null) {
                throw new RuntimeException("JSON document has no \"contents\" field");
            }
            return contents;
        }

        @Override
        public String raw() {
           return contents();
        }

        @Override
        public boolean indexable() {
            return true;
        }

        @Override
        public Map<String, String> fields() {
            return fields;
        }

        /**
         * Handles the process of getting the right information out of a TREC CDS XML document.
         * Practically creates a map of of elementName -> elementValue for a set of pre-defined fields; e.g.,
         * title, abstract, etc.
         */
        private class ClinicalXMLDocumentHandler extends DefaultHandler {
            String[] acceptedElements = new String[] {"article-id", "article-title", "abstract", "body"};
            HashMap<String, String> articleContents = new HashMap<String, String>();

            String articleIdType;
            String elementValue;

            @Override
            public void startElement (String uri, String localName, String qName, Attributes attributes) {
                // TODO: handle checking of element to get the right id here
                if(qName.equalsIgnoreCase("article-id")) {
                    articleIdType = attributes.getValue("pub-id-type");
                }
            }

            @Override
            public void endElement(String uri, String localName, String qName) throws SAXException {
                if(Arrays.asList(acceptedElements).contains(qName)) {
                    if(qName.equalsIgnoreCase("article-id")) {
                        qName = qName + "-" + articleIdType;
                    }
                    articleContents.put(qName.toLowerCase(), elementValue);
                }
            }

            @Override
            public void characters(char[] ch, int start, int length) throws SAXException {
                elementValue = new String(ch, start, length);
            }

            public HashMap<String, String> getArticleContents() { return this.articleContents; }
        };
    }


}
