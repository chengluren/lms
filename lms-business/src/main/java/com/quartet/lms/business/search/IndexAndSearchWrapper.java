package com.quartet.lms.business.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

import java.util.List;
import java.util.Map;

/**
 * Created by lcheng on 2014/5/2.
 */
public interface IndexAndSearchWrapper {

    public void index(Document doc);

    public void index(List<Document> docs);

    public void reIndex(Term idTerm, Document doc);

    public void deleteIndex(Term idTerm);

    public void truncate();

    public long count(Query query);

    public List<Document> search(Query query, SortField[] sortFields, int firstResult, int pageSize);

    public void optimize();

    public void release();

    /**
     * 获得索引基础统计信息
     * @return
     */
    public Map<String,?> getIndexStats();
}
