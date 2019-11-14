package com.globalegrow.tianchi.business.bts.sink.es;

import org.elasticsearch.action.ActionRequest;

import java.io.Serializable;

public interface RequestIndexer extends Serializable {
    void add(ActionRequest... actionRequests);
}