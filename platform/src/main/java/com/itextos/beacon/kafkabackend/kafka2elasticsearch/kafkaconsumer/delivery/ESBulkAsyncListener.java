package com.itextos.beacon.kafkabackend.kafka2elasticsearch.kafkaconsumer.delivery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

import com.itextos.beacon.errorlog.K2ESLog;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class ESBulkAsyncListener
        implements
        BiConsumer<BulkResponse, Throwable>
{

    private static final K2ESLog log = K2ESLog.getInstance();
    protected List<BulkOperation> bulkOperations;
    protected boolean isFullMsg = false;

    public ESBulkAsyncListener(
            List<BulkOperation> operations,
            boolean aIsFullMsg)
    {
        bulkOperations = operations;
        isFullMsg = aIsFullMsg;
    }

    @Override
    public void accept(BulkResponse bulkResponse, Throwable throwable) {
        if (throwable != null) {
            // Handle overall failure
            onFailure(throwable);
            return;
        }
        
        if (bulkResponse != null) {
            onResponse(bulkResponse);
        }
    }

    public void onResponse(BulkResponse aResponse) {
        if (aResponse.errors()) {
            final List<BulkResponseItem> items = aResponse.items();

            for (int rIdx = 0; rIdx < items.size(); rIdx++) {
                final BulkResponseItem response = items.get(rIdx);

                if (response.error() != null) {
                    final String errorType = response.error().type();
                    final String errorReason = response.error().reason();
                    final String operationId = response.id() != null ? response.id() : "unknown";
                    final String indexName = response.index() != null ? response.index() : "unknown";

                    if (isFullMsg) {
                        log.error("FullMsg, Failed to Index[" + operationId + "] in index[" + indexName + "]: " + 
                                 "Type: " + errorType + ", Reason: " + errorReason);
                        
                        if (log.isDebugEnabled()) {
                            final BulkOperation operation = bulkOperations.get(rIdx);
                            log.debug("FullMsg, Operation[" + operationId + "]: " + operation.toString());
                        }
                    } else {
                        log.error("Failed to Index[" + operationId + "] in index[" + indexName + "]: " + 
                                 "Type: " + errorType + ", Reason: " + errorReason);
                        
                        if (log.isDebugEnabled()) {
                            final BulkOperation operation = bulkOperations.get(rIdx);
                            log.debug("Operation[" + operationId + "]: " + operation.toString());
                        }
                    }
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Bulk operation completed successfully for " + 
                         (isFullMsg ? "full message index" : "main index"));
            }
        }
    }

    public void onFailure(Throwable aException) {
        log.error("Bulk operation failed for " + 
                 (isFullMsg ? "full message index: " : "main index: ") + 
                 aException.getMessage(), aException);
    }

    // Alternative method for use with CompletableFuture
    public static CompletableFuture<BulkResponse> processAsync(
            CompletableFuture<BulkResponse> future,
            List<BulkOperation> operations,
            boolean isFullMsg) {
        
        ESBulkAsyncListener listener = new ESBulkAsyncListener(operations, isFullMsg);
        return future.whenComplete(listener);
    }
}