/*
 * Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.log4j.helpers;

import com.amazonaws.AmazonClientException;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Kinesis PutRecords helper class to build and send batch of records.
 */
public class AmazonKinesisPutRecordsHelper {

    private static final Log LOG = LogFactory.getLog(AmazonKinesisPutRecordsHelper.class);
    // Count limit for how many records could be put in one request.
    private static final int RECORDS_COUNT_LIMIT_FOR_ONE_BATCH = 500;

    private final AmazonKinesisAsyncClient amazonKinesisClient;
    private final String streamName;
    private final boolean isUsingSequenceNumberForOrdering;
    private String sequenceNumberForOrdering;
    private AsyncHandler<PutRecordsRequest, PutRecordsResult> asyncCallHander;
    private int batchSize = RECORDS_COUNT_LIMIT_FOR_ONE_BATCH ;

    public int getBatchSize() {
        return batchSize;
    }

    // Synchronized request list for thread-safe usage.
    private List<PutRecordsRequestEntry> putRecordsRequestEntryList =
            Collections.synchronizedList(new ArrayList<PutRecordsRequestEntry>());

    /**
     * Constructor. By calling this constructor, helper would not set sequenceNumberForOrdering for
     * each request.
     * @param amazonKinesisClient          Amazon Kinesis Client.
     * @param streamName                   Stream Name.
     */
    public AmazonKinesisPutRecordsHelper(AmazonKinesisAsyncClient amazonKinesisClient,
                                         String streamName,
                                         int batchSize) {
        this(amazonKinesisClient, streamName, null, false, batchSize);
    }

    /**
     * Constructor. By calling this constructor, helper would set sequenceNumberForOrdering for each
     * request. If the initialSequenceNumberForOrdering is null, helper will send a request without it
     * first, and set the sequence number got from the result list.
     * @param amazonKinesisClient                  Amazon Kinesis Client.
     * @param streamName                           Stream Name.
     * @param initialSequenceNumberForOrdering     Initial Sequence Number For Ordering.
     */
    public AmazonKinesisPutRecordsHelper(AmazonKinesisAsyncClient amazonKinesisClient,
                                         String streamName,
                                         String initialSequenceNumberForOrdering) {
        this(amazonKinesisClient, streamName, initialSequenceNumberForOrdering, true,
                RECORDS_COUNT_LIMIT_FOR_ONE_BATCH
                );
    }

    /**
     * Constructor.
     * @param amazonKinesisClient                  Amazon Kinesis Client.
     * @param streamName                           Stream Name.
     * @param initialSequenceNumberForOrdering     Initial Sequence Number For Ordering.
     * @param isUsingSequenceNumberForOrdering     If Using Seqeuence Number For Ordering.
     */
    AmazonKinesisPutRecordsHelper(AmazonKinesisAsyncClient amazonKinesisClient,
                                  String streamName,
                                  String initialSequenceNumberForOrdering,
                                  boolean isUsingSequenceNumberForOrdering,
                                  int batchSize
                                  ) {
        this.amazonKinesisClient = amazonKinesisClient;
        this.asyncCallHander = new AsyncBatchPutHandler(streamName, this);
        this.streamName = streamName;
        this.sequenceNumberForOrdering = initialSequenceNumberForOrdering;
        this.isUsingSequenceNumberForOrdering = isUsingSequenceNumberForOrdering;
        this.batchSize = batchSize;
    }

    /**
     * Add record into request batch.
     * @param data              Record Data.
     * @param partitionKey      Record PartitionKey.
     * @param explicitHashKey   Record ExplicitHashKey.
     * @return True if request batch is full.
     */
    public boolean addRecord(ByteBuffer data, String partitionKey, String explicitHashKey) {
        // Initialize PutRecords request entry and add it to request entry list.
        PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
        putRecordsRequestEntry.setData(data);
        putRecordsRequestEntry.setPartitionKey(partitionKey);
        putRecordsRequestEntry.setExplicitHashKey(explicitHashKey);
        putRecordsRequestEntryList.add(putRecordsRequestEntry);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Add Record : %s", putRecordsRequestEntry.toString()));
        }

        // Return true if the entries count hit the limit, otherwise, return false.
        return (putRecordsRequestEntryList.size() >= getBatchSize());
    }


    public boolean sendRecordsAsync() {
        // Only try to put records if there are some records already in cache.
        if (putRecordsRequestEntryList.size() > 0) {
            // Calculate the real number of records which will be put in the request. If the number of records in
            // the list is no less than 500, set it to 500; otherwise, set it as the list size.
            final int intendToSendRecordNumber =
                    (putRecordsRequestEntryList.size() >= RECORDS_COUNT_LIMIT_FOR_ONE_BATCH) ?
                            RECORDS_COUNT_LIMIT_FOR_ONE_BATCH : putRecordsRequestEntryList.size();
            try {
                // Create PutRecords request and use kinesis client to send it.
                PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
                putRecordsRequest.setStreamName(streamName);
                // Only set sequenceNumberForOrdering if required by users.
                if (isUsingSequenceNumberForOrdering) {
                    putRecordsRequest.setSequenceNumberForOrdering(sequenceNumberForOrdering);
                }
                // Set a sub list of the current records list with maximum of 500 records.
                putRecordsRequest.setRecords(putRecordsRequestEntryList.subList(0, intendToSendRecordNumber));
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("SequenceNumberForOrdering : [%s]; NumberOfRecords : [%d]",
                            sequenceNumberForOrdering, intendToSendRecordNumber));
                }
                amazonKinesisClient.putRecordsAsync(putRecordsRequest, asyncCallHander);
            } catch (AmazonClientException e) {
                LOG.error(e.getMessage());
            }
        } else {
            LOG.warn("There is no record in batch.");
        }

        // Return true if the entries count hit the limit, otherwise, return false.
        return (putRecordsRequestEntryList.size() >= RECORDS_COUNT_LIMIT_FOR_ONE_BATCH);
    }

    public int getSuccessCountAndaddFailedRecordsBackToQueue(PutRecordsRequest putRecordsRequst,  PutRecordsResult putRecordsResult) {
    /*
     * Handle PutRecordsResult and prepare for next batch. Each failed record will be put into
     * next request in order. In this helper, we remove the succeed records from list which leave
     * failed records in the list with the same order as before. Synchronize iteration here for
     * thread-safe of handling results.
     */
        int totalSucceedRecordCount = 0;
        synchronized (putRecordsRequestEntryList) {
            Iterator<PutRecordsRequestEntry> putRecordsRequestEntryIterator =
                    putRecordsRequst.getRecords().iterator();
            for (PutRecordsResultEntry putRecordsResultEntry : putRecordsResult.getRecords()) {
                final String message = putRecordsResultEntry.getMessage();
                final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryIterator.next();

                // If message equals to null, it means the record succeed in putting.
                if (message == null) {
                    // Keep tracking the last sequence number in each batch.
                    if (isUsingSequenceNumberForOrdering) {
                        sequenceNumberForOrdering = putRecordsResultEntry.getSequenceNumber();
                    }

                    totalSucceedRecordCount++;
                    putRecordsRequestEntryIterator.remove();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Succeed Record : %s", putRecordsResultEntry.toString()));
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Failed Record : %s with error %s",
                                putRecordRequestEntry.toString(), message));
                    }
                }
            }
            putRecordsRequestEntryList.addAll(putRecordsRequst.getRecords());
        }
        return totalSucceedRecordCount;
    }
}
