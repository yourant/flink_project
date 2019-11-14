package com.globalegrow.tianchi.business.bts.sink.es;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;

public class BulkProcessorIndexer implements RequestIndexer {
    private final BulkProcessor bulkProcessor;

    public BulkProcessorIndexer(BulkProcessor bulkProcessor) {
        this.bulkProcessor = bulkProcessor;
    }


    @Override
    public void add(ActionRequest... actionRequests) {
        for (ActionRequest actionRequest : actionRequests) {
            this.bulkProcessor.add(actionRequest);
        }
    }
}