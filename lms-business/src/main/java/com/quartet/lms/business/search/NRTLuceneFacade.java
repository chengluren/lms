package com.quartet.lms.business.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by lcheng on 2014/5/2.
 */
public class NRTLuceneFacade implements IndexAndSearchWrapper{
    private static Version LUCENE_VERSION = Version.LUCENE_47;
    private static String P_INDEX_PATH = "index.path";
    private static String P_RAM_BUF_SIZE = "index.ramBufferSize";

    private IndexWriter writer;
    private TrackingIndexWriter trackingWriter;
    private ReferenceManager<IndexSearcher> searcherRM;
    private ControlledRealTimeReopenThread reopenThread;
    private Directory indexDirectory;

    private Analyzer analyzer;

    private String indexPath;
    private int ramBufferSize;

    private volatile long reopenToken;

    private NRTLuceneFacade() {
        //indexPath = SysUtils.getConfigValue(P_INDEX_PATH);
        indexPath = P_INDEX_PATH;
        ramBufferSize = 1;
        analyzer = new IKAnalyzer(true);
        try {
            indexDirectory = FSDirectory.open(new File(indexPath));
            IndexWriterConfig iwc = new IndexWriterConfig(LUCENE_VERSION, analyzer);
            iwc.setRAMBufferSizeMB(ramBufferSize);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            if (IndexWriter.isLocked(indexDirectory)){
                IndexWriter.unlock(indexDirectory);
            }
            writer = new IndexWriter(indexDirectory, iwc);
            trackingWriter = new TrackingIndexWriter(writer);
            searcherRM = new SearcherManager(writer, true, null);
            reopenThread = new ControlledRealTimeReopenThread(trackingWriter, searcherRM, 60.00, 0.1);
            reopenThread.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static class NRTLuceneFacadeHolder {
        private static NRTLuceneFacade instance = new NRTLuceneFacade();
    }

    public static NRTLuceneFacade instance() {
        return NRTLuceneFacadeHolder.instance;
    }

    public void index(Document doc) {
        try {
            reopenToken = trackingWriter.addDocument(doc);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            commit();
        }
    }

    public void index(List<Document> docs) {
        try {
            reopenToken = trackingWriter.addDocuments(docs);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            commit();
        }
    }

    public void reIndex(Term idTerm, Document doc) {
        try {
            reopenToken = trackingWriter.updateDocument(idTerm, doc);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            commit();
        }
    }

    public void deleteIndex(Term idTerm) {
        try {
            reopenToken = trackingWriter.deleteDocuments(idTerm);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            commit();
        }
    }

    public void truncate() {
        try {
            reopenToken = trackingWriter.deleteAll();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            commit();
        }
    }

    public long count(Query query) {
        long count = 0;
        try {
            reopenThread.waitForGeneration(reopenToken);
            IndexSearcher searcher = null;
            try {
                searcher = searcherRM.acquire();
                TotalHitCountCollector counter = new TotalHitCountCollector();
                searcher.search(query, counter);
                return counter.getTotalHits();
            } finally {
                if (searcher != null) {
                    searcherRM.release(searcher);
                }
            }

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    public List<Document> search(Query query, SortField[] sortFields, int firstResult, int pageSize) {
        try {
            List<Document> result = new ArrayList<>();
            IndexSearcher searcher = null;
            try {
                searcher = searcherRM.acquire();
                Sort theSort = null;
                if (sortFields != null) {
                    theSort = new Sort(sortFields);
                }
                int totalResults = firstResult + pageSize;
                TopDocs tds = (theSort != null) ? searcher.search(query, totalResults, theSort) : searcher.search(query, totalResults);
                ScoreDoc[] scoreDocs = tds.scoreDocs;

                for (int i = firstResult; (i <= (firstResult + pageSize) && i < scoreDocs.length); i++) {
                    result.add(searcher.doc(scoreDocs[i].doc));
                }
            } finally {
                if (searcher != null) {
                    searcherRM.release(searcher);
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

    public QueryParser newQueryParser(String field) {
        return new QueryParser(LUCENE_VERSION, field, analyzer);
    }

    public void release() {
        reopenThread.interrupt();
        reopenThread.close();

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

    private void commit() {
        try {
            writer.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
