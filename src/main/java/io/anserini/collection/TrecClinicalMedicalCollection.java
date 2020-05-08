package io.anserini.collection;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;

public class TrecClinicalMedicalCollection extends DocumentCollection<TrecClinicalMedicalCollection.Document> {

    private static final Logger LOG = LogManager.getLogger(TrecClinicalMedicalCollection.class);

    public TrecClinicalMedicalCollection(Path path) {
        this.allowedFileSuffix = new HashSet<>(Arrays.asList(".nxml", ".xml"));
        this.path = path;
    }

    @Override
    public FileSegment<TrecClinicalMedicalCollection.Document> createFileSegment(Path p) throws IOException {
        return new Segment(p);
    }

    /**
     * A file in a TREC CDS collection. Each file contains only one article and, therefore, one Document.
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

    /**
     * A document in TREC CDS - ie one article with multiple fields.
     */
    public static class Document extends MultifieldSourceDocument {
        static final String[] acceptedElements = new String[]{"article-id", "article-title", "abstract", "body", "journal-title", "year"};

        private String fileName;
        private String id;
        private String contents;
        private Map<String, String> fields;


        public Document(BufferedReader bRdr, String fileName) throws IOException {
            this.fileName = fileName;
            this.fields = new HashMap<String, String>();

            Path path = Paths.get(fileName);
            org.jsoup.nodes.Document doc = Jsoup.parse(new String(Files.readAllBytes(path)), "", Parser.xmlParser());

            // look for each of the fields we want
            for (String fieldName : acceptedElements) {
                Elements elements = doc.select(fieldName);
                if (elements.size() > 0) {
                    fields.put(fieldName, elements.get(0).text());
                }
            }

            this.id = findBestId();
            contents = StringUtils.join(fields, " ");
        }

        /**
         * Find the best id from the list of possible ones.
         *
         * @return id as String.
         */
        private String findBestId() {
            String theId = this.fileName;
            if (fields.containsKey("article-id-pmid")) {
                theId = fields.get("article-id-pmid");
            } else if (fields.containsKey("article-id-pmc")) {
                theId = fields.get("article-id-pmc");
            } else if (fields.containsKey("article-id-publisher-id")) {
                theId = fields.get("article-id-publisher-id");
            } else if (fields.containsKey("article-id-doi")) {
                theId = fields.get("article-id-doi");
            }
            return theId;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public String contents() {
            if (contents == null) {
                throw new RuntimeException("XML document has no \"contents\" field");
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

    }

}
