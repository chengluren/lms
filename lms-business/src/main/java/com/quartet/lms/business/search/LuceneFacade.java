package com.quartet.lms.business.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lcheng
 * @version 1.0
 *          ${tags}
 */
public class LuceneFacade implements IndexAndSearchWrapper {
    private static Version LUCENE_VERSION = Version.LUCENE_47;

    private String P_INDEX_PATH = "index.path";
    private String P_RAM_BUF_SIZE = "index.ramBufferSize";
    private Analyzer analyzer;
    private String indexPath;
    private int ramBufferSize;

    private IndexWriter writer;
    private IndexReader reader;
    private Directory directory;

    private Object readerLock;

    private LuceneFacade() {
        analyzer = new IKAnalyzer(true);
//        analyzer = new MMSegAnalyzer();
//        indexPath = SpringUtils.getConfigValue(P_INDEX_PATH);
//        ramBufferSize = Integer.valueOf(SpringUtils.getConfigValue(P_RAM_BUF_SIZE, "64"));
        indexPath = "D:/exam/index";
        ramBufferSize = 1;

        try {
            directory = FSDirectory.open(new File(indexPath));
            IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, analyzer);
            config.setRAMBufferSizeMB(ramBufferSize);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            if (IndexWriter.isLocked(directory)){
                IndexWriter.unlock(directory);
            }
            writer = new IndexWriter(directory, config);
            readerLock = new Object();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static class LuceneFacadeHolder {
        private static LuceneFacade instance = new LuceneFacade();
    }

    public static LuceneFacade instance() {
        return LuceneFacadeHolder.instance;
    }

    public IndexWriter getIndexWriter() {
        return writer;
    }

    public void index(Document doc) {
        try {
            writer.addDocument(doc);
            commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void index(List<Document> docs) {
        try {
            writer.addDocuments(docs);
            commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void reIndex(Term idTerm, Document doc) {
        try {
            writer.updateDocument(idTerm, doc);
            commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteIndex(Term idTerm) {
        try {
            writer.deleteDocuments(idTerm);
            commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void truncate() {
        try {
            writer.deleteAll();
            commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long count(Query query) {
        long count = 0;
        try {
            synchronized (readerLock) {
                if (reader==null){
                    reader = DirectoryReader.open(directory);
                }
                IndexReader newReader = DirectoryReader.openIfChanged((DirectoryReader) reader);
                if (newReader != null) {
                    reader.close();
                    reader = newReader;
                }
                IndexSearcher searcher = new IndexSearcher(reader);
                TotalHitCountCollector counter = new TotalHitCountCollector();
                searcher.search(query, counter);
                return counter.getTotalHits();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    public List<Document> search(Query query, SortField[] sortFields, int firstResult, int pageSize) {
        try {
            List<Document> result = new ArrayList<>();
            IndexSearcher searcher = null;
            ScoreDoc[] scoreDocs = null;
            synchronized (readerLock) {
                if (reader==null){
                    reader = DirectoryReader.open(directory);
                }
                IndexReader newReader = DirectoryReader.openIfChanged((DirectoryReader) reader);
                if (newReader != null) {
                    reader.close();
                    reader = newReader;
                }
                searcher = new IndexSearcher(reader);

                Sort theSort = null;
                if (sortFields != null) {
                    theSort = new Sort(sortFields);
                }
                int totalResults = firstResult + pageSize;
                TopDocs tds = (theSort != null) ? searcher.search(query, totalResults, theSort) : searcher.search(query, totalResults);
                scoreDocs = tds.scoreDocs;
            }
            if (scoreDocs != null) {
                for (int i = firstResult; (i <= (firstResult + pageSize) && i < scoreDocs.length); i++) {
                    result.add(searcher.doc(scoreDocs[i].doc));
                }
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void optimize() {
        try {
            writer.forceMerge(1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void release() {

        try {
            writer.commit();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, ?> getIndexStats() {
        Map<String,Object> stats = new HashMap<>();
        stats.put("numDocs",writer.numDocs());
        stats.put("numRamDocs",writer.numRamDocs());
        stats.put("maxDoc",writer.maxDoc());
        stats.put("hasDeletions",writer.hasDeletions());
        stats.put("hasUncommittedChanges",writer.hasUncommittedChanges());
        return stats;
    }


    public QueryParser newQueryParser(String field) {
        return new QueryParser(LUCENE_VERSION, field, analyzer);
    }

    private void commit() {
        try {
            writer.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
