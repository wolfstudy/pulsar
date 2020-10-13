/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DateSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.InstantSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.LocalDateSchema;
import org.apache.pulsar.client.impl.schema.LocalDateTimeSchema;
import org.apache.pulsar.client.impl.schema.LocalTimeSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.TimeSchema;
import org.apache.pulsar.client.impl.schema.TimestampSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.*;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class TopicsBase extends PersistentTopicsBase {

    private static final Random random = new Random(System.currentTimeMillis());

    private static String DEFAULT_PRODUCER_NAME = "RestProducer";

    private ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<Integer>> owningTopics = new ConcurrentOpenHashMap<>();

    private ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Schema>> subscriptions = new ConcurrentOpenHashMap<>();


    protected  void publishMessages(AsyncResponse asyncResponse, ProduceMessageRequest request, boolean authoritative) {
        String topic = topicName.getPartitionedTopicName();
        if (owningTopics.containsKey(topic)) {
            // if broker owns some of the partitions then proceed to publish message
            publishMessagesToMultiplePartitions(topicName, request, owningTopics.get(topicName.getPartitionedTopicName()), asyncResponse);
        } else {
            if (!findOwnerBrokerForTopic(authoritative, asyncResponse)) {
                publishMessagesToMultiplePartitions(topicName, request, owningTopics.get(topicName.getPartitionedTopicName()), asyncResponse);
            }
        }
    }

    protected void publishMessagesToPartition(AsyncResponse asyncResponse, ProduceMessageRequest request,
                                                     boolean authoritative, int partition) {
        String topic = topicName.getPartitionedTopicName();
        if (owningTopics.containsKey(topic) && owningTopics.get(topic).contains(partition)) {
            // If broker owns the partition then proceed to publish message
            publishMessagesToSinglePartition(topicName, request, partition, asyncResponse);
        } else {
            if (!findOwnerBrokerForTopic(authoritative, asyncResponse)) {
                publishMessagesToSinglePartition(topicName, request, partition, asyncResponse);
            }
        }
    }

    private CompletableFuture<PositionImpl> publishSingleMessageToSinglePartition(String topic, Message message, String producerName) {
        CompletableFuture<PositionImpl> publishResult = new CompletableFuture<>();
        pulsar().getBrokerService().getTopic(topic, false).thenAccept(t -> {
            if (!t.isPresent()) {
                publishResult.completeExceptionally(new BrokerServiceException.TopicNotFoundException("Topic not owned by current broker."));
            } else {
                t.get().publishMessage(messageToByteBuf(message, producerName),
                        RestMessagePublishContext.get(publishResult, t.get(), System.nanoTime()));
            }
        });

        return publishResult;
    }

    private void publishMessagesToSinglePartition(TopicName topicName, ProduceMessageRequest request,
                                                  int partition, AsyncResponse asyncResponse) {
        try {
            String producerName = request.getProducerName().isEmpty()? DEFAULT_PRODUCER_NAME : request.getProducerName();
            List<Message> messages = buildMessage(request);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProduceMessageResponse.ProduceMessageResult> produceMessageResults = new ArrayList<>();
            for (int index = 0; index < messages.size(); index++) {
                ProduceMessageResponse.ProduceMessageResult produceMessageResult = new ProduceMessageResponse.ProduceMessageResult();
                produceMessageResult.setPartition(partition);
                produceMessageResults.add(produceMessageResult);
                publishSingleMessageToSinglePartition(topicName.getPartition(partition).getLocalName(), messages.get(index),
                        producerName);
            }
            FutureUtil.waitForAll(publishResults);
            processPublishMessageResults(produceMessageResults, publishResults);
            asyncResponse.resume(new ProduceMessageResponse(produceMessageResults));
        } catch (JsonProcessingException e) {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to deserialize messages to publish."));
        }
    }

    private void publishMessagesToMultiplePartitions(TopicName topicName, ProduceMessageRequest request,
                                                     ConcurrentOpenHashSet<Integer> partitionIndexes,
                                                     AsyncResponse asyncResponse) {
        try {
            String producerName = request.getProducerName().isEmpty()? DEFAULT_PRODUCER_NAME : request.getProducerName();
            List<Message> messages = buildMessage(request);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProduceMessageResponse.ProduceMessageResult> produceMessageResults = new ArrayList<>();
            List<Integer> owningPartitions = partitionIndexes.values();
            for (int index = 0; index < messages.size(); index++) {
                ProduceMessageResponse.ProduceMessageResult produceMessageResult = new ProduceMessageResponse.ProduceMessageResult();
                produceMessageResult.setPartition(owningPartitions.get(index % (int)partitionIndexes.size()));
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishSingleMessageToSinglePartition(topicName.getPartition(owningPartitions.get(index % (int)partitionIndexes.size())).getLocalName(),
                    messages.get(index), producerName));
            }
            FutureUtil.waitForAll(publishResults);
            processPublishMessageResults(produceMessageResults, publishResults);
            asyncResponse.resume(new ProduceMessageResponse(produceMessageResults));
        } catch (JsonProcessingException e) {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to deserialize messages to publish."));
        }
    }

    private void processPublishMessageResults(List<ProduceMessageResponse.ProduceMessageResult> produceMessageResults,
                                              List<CompletableFuture<PositionImpl>> publishResults) {
        // process publish message result
        for (int index = 0; index < produceMessageResults.size(); index++) {
            try {
                PositionImpl position = publishResults.get(index).get();
                produceMessageResults.get(index).setMessageId(position.toString());
                log.info("Successfully publish [{}] message with rest produce message request for topic  {}: {} ",
                        index, topicName, position);
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.warn("Fail publish [{}] message with rest produce message request for topic  {}: {} ",
                            index, topicName);
                }
                if (e instanceof BrokerServiceException.TopicNotFoundException) {
                    // Topic ownership might changed, force to look up again.
                    owningTopics.remove(topicName.getPartitionedTopicName());
                }
                extractException(e, produceMessageResults.get(index));
            }
        }
    }

    private void extractException(Exception e, ProduceMessageResponse.ProduceMessageResult produceMessageResult) {
        if (!(e instanceof BrokerServiceException.TopicFencedException || e instanceof ManagedLedgerException)) {
            produceMessageResult.setErrorCode(2);
        } else {
            produceMessageResult.setErrorCode(1);
        }
        produceMessageResult.setError(e.getMessage());
    }

    // Look up topic owner for given topic. Return true if asyncResponse has been completed.
    private boolean findOwnerBrokerForTopic(boolean authoritative, AsyncResponse asyncResponse) {
        PartitionedTopicMetadata metadata = internalGetPartitionedMetadata(authoritative, false);
        List<String> redirectAddresses = Collections.synchronizedList(new ArrayList<>());

        if (!topicName.isPartitioned() && metadata.partitions > 1) {
            // Partitioned topic with multiple partitions, need to do look up for each partition.
            for (int index = 0; index < metadata.partitions; index++) {
                lookUpBrokerForTopic(topicName.getPartition(index), authoritative, redirectAddresses);
            }
        } else {
            // Non-partitioned topic or specific topic partition.
            lookUpBrokerForTopic(topicName, authoritative, redirectAddresses);
        }

        // Current broker doesn't own the topic or any partition of the topic, redirect client to a broker
        // that own partition of the topic or know who own partition of the topic.
        if (!owningTopics.containsKey(topicName.getPartitionedTopicName())) {
            if (redirectAddresses.isEmpty()) {
                // No broker to redirect, means look up for some partitions failed, client should retry with other brokers.
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Can't find owner of given topic."));
                return true;
            } else {
                // Redirect client to other broker owns the topic or know which broker own the topic.
                try {
                    URI redirectURI = new URI(String.format("%s%s", redirectAddresses.get(0), uri.getPath()));
                    asyncResponse.resume(Response.temporaryRedirect(redirectURI));
                    return true;
                } catch (URISyntaxException | NullPointerException e) {
                    log.error("Error in preparing redirect url with rest produce message request for topic  {}: {}",
                            topicName, e.getMessage(), e);
                    asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, "Fail to redirect client request."));
                    return true;
                }
            }
        }

        return false;
    }

    // Look up topic owner for non-partitioned topic or single topic partition.
    private void lookUpBrokerForTopic(TopicName partitionedTopicName, boolean authoritative, List<String> redirectAddresses) {
        CompletableFuture<Optional<LookupResult>> lookupFuture = pulsar().getNamespaceService()
                .getBrokerServiceUrlAsync(partitionedTopicName, LookupOptions.builder().authoritative(authoritative).loadTopicsInBundle(false).build());

        lookupFuture.thenAccept(optionalResult -> {
            if (optionalResult == null || !optionalResult.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.debug("Fail to lookup topic for rest produce message request for topic {}, current broker is owner broker: {}",
                            partitionedTopicName);
                }
                completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses);
                return;
            }

            LookupResult result = optionalResult.get();

            if (result.getLookupData().getHttpUrl().equals(pulsar().getWebServiceAddress())) {
                pulsar().getBrokerService().getLookupRequestSemaphore().release();
                // Current broker owns the topic, add to owning topic.
                if (log.isDebugEnabled()) {
                    log.debug("Complete topic look up for rest produce message request for topic {}, current broker is owner broker: {}",
                            partitionedTopicName, result.getLookupData());
                }
                owningTopics.computeIfAbsent(partitionedTopicName.getPartitionedTopicName(),
                        (key) -> new ConcurrentOpenHashSet<Integer>()).add(partitionedTopicName.getPartitionIndex());
                completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses);
            } else {
                // Current broker doesn't own the topic or doesn't know who own the topic.
                if (log.isDebugEnabled()) {
                    log.debug("Complete topic look up for rest produce message request for topic {}, current broker is not owner broker: {}",
                            partitionedTopicName, result.getLookupData());
                }
                if (result.isRedirect()) {
                    // Redirect lookup.
                    completeLookup(Pair.of(Arrays.asList(result.getLookupData().getHttpUrl(),
                            result.getLookupData().getHttpUrlTls()), false), redirectAddresses);
                } else {
                    // Found owner for topic.
                    completeLookup(Pair.of(Arrays.asList(result.getLookupData().getHttpUrl(),
                            result.getLookupData().getHttpUrlTls()), true), redirectAddresses);
                }
            }
        }).exceptionally(exception -> {
            log.warn("Failed to lookup broker with rest produce message request for topic {}: {}",
                    partitionedTopicName, exception.getMessage(), exception);
            completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses);
            return null;
        });
    }

    private Schema getSchema(String keySchemaJson, String valueSchemaJson) {
        Schema keySchema = getSchemaFromJson(keySchemaJson);
        Schema valueSchema = getSchemaFromJson(valueSchemaJson);
        return KeyValueSchema.of(keySchema, valueSchema);
    }

    private Schema getSchemaFromJson(String schema) {
        SchemaInfo schemaInfo;
        try {
            schemaInfo = ObjectMapperFactory.getThreadLocal().readValue(schema, SchemaInfo.class);
            switch (schemaInfo.getType()) {
                case INT8:
                    return ByteSchema.of();
                case INT16:
                    return ShortSchema.of();
                case INT32:
                    return IntSchema.of();
                case INT64:
                    return LongSchema.of();
                case STRING:
                    return StringSchema.utf8();
                case FLOAT:
                    return FloatSchema.of();
                case DOUBLE:
                    return DoubleSchema.of();
                case BOOLEAN:
                    return BooleanSchema.of();
                case BYTES:
                    return BytesSchema.of();
                case DATE:
                    return DateSchema.of();
                case TIME:
                    return TimeSchema.of();
                case TIMESTAMP:
                    return TimestampSchema.of();
                case INSTANT:
                    return InstantSchema.of();
                case LOCAL_DATE:
                    return LocalDateSchema.of();
                case LOCAL_TIME:
                    return LocalTimeSchema.of();
                case LOCAL_DATE_TIME:
                    return LocalDateTimeSchema.of();
                case JSON:
                case AVRO:
                    return GenericSchemaImpl.of(schemaInfo);
                case KEY_VALUE:
                default:
                    throw new IllegalArgumentException("Schema type of '"  + schemaInfo.getType() +
                            "' is not supported yet");
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new AutoProduceBytesSchema();
    }

    // Convert message to ByteBuf
    public ByteBuf messageToByteBuf(Message message, String producerName) {
        checkArgument(message instanceof MessageImpl, "Message must be type of MessageImpl.");

        MessageImpl msg = (MessageImpl) message;
        PulsarApi.MessageMetadata.Builder msgMetadataBuilder = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();

        // filled in required fields
        if (!msgMetadataBuilder.hasSequenceId()) {
            msgMetadataBuilder.setSequenceId(-1);
        }
        if (!msgMetadataBuilder.hasPublishTime()) {
            msgMetadataBuilder.setPublishTime(System.currentTimeMillis());
        }
        if (!msgMetadataBuilder.hasProducerName()) {
            msgMetadataBuilder.setProducerName(producerName);
        }

        msgMetadataBuilder.setCompression( CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        msgMetadataBuilder.setUncompressedSize(payload.readableBytes());
        PulsarApi.MessageMetadata msgMetadata = msgMetadataBuilder.build();

        ByteBuf byteBuf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, msgMetadata, payload);

        msgMetadataBuilder.recycle();
        msgMetadata.recycle();

        return byteBuf;
    }

    // Build pulsar message from serialized message.
    private List<Message> buildMessage(ProduceMessageRequest produceMessageRequest) throws JsonProcessingException {
        List<RestProduceMessage> messages;
        List<Message> pulsarMessages = new ArrayList<>();
        Schema schema = getSchema(produceMessageRequest.getKeySchema(), produceMessageRequest.getValueSchema());

        try {
            messages = ObjectMapperFactory.getThreadLocal().readValue(produceMessageRequest.getMessages(), new TypeReference<List<RestProduceMessage>>(){});
            for (RestProduceMessage message : messages) {
                PulsarApi.MessageMetadata.Builder metadataBuilder = PulsarApi.MessageMetadata.newBuilder();
                metadataBuilder.addAllReplicateTo(message.getReplicationClusters());
                metadataBuilder.setPartitionKey(message.getKey());
                metadataBuilder.setEventTime(message.getEventTime());
                metadataBuilder.setSequenceId(message.getSequenceId());
                if (message.getDeliverAt() != 0) {
                    metadataBuilder.setDeliverAtTime(message.getDeliverAt());
                } else if (message.getDeliverAfterMs() != 0) {
                    metadataBuilder.setDeliverAtTime(message.getEventTime() + message.getDeliverAfterMs());
                }
                pulsarMessages.add(MessageImpl.create(metadataBuilder, ByteBuffer.wrap(message.getValue().getBytes(UTF_8)), schema));
            }
        } catch (JsonProcessingException e) {
            if (log.isDebugEnabled()) {
                log.warn("Failed to deserialize message with rest produce message request for topic {}: {}",
                        topicName, produceMessageRequest.getMessages());
                throw e;
            }
        }

        return pulsarMessages;
    }

    private synchronized void completeLookup( Pair<List<String>, Boolean> result, List<String> redirectAddresses) {
        pulsar().getBrokerService().getLookupRequestSemaphore().release();
        if (!result.getLeft().isEmpty()) {
            if (result.getRight()) {
                // If address is for owner of topic partition, add to head and it'll have higher priority
                // compare to broker for look redirect.
                redirectAddresses.add(0, isRequestHttps() ? result.getLeft().get(1) : result.getLeft().get(0));
            } else {
                redirectAddresses.add(redirectAddresses.size(), isRequestHttps() ? result.getLeft().get(1) : result.getLeft().get(0));
            }
        }
    }





    // Consume messages
    protected void createConsumer(AsyncResponse asyncResponse, CreateConsumerRequest request,
                                  boolean authoritative, String subscriptionName) {
        String topic = topicName.getPartitionedTopicName();
        if (owningTopics.containsKey(topic)) {
            internalCreateConsumer(asyncResponse, request, owningTopics.get(topic), subscriptionName);
        } else {
            if (!findOwnerBrokerForTopic(authoritative, asyncResponse)) {
                internalCreateConsumer(asyncResponse, request, owningTopics.get(topic), subscriptionName);
            }
        }
    }

    // For rest api, adding a consumer is only adding the consumer name to subscriptions map to keep track of
    // consumers according to the subscription type.
    private void internalCreateConsumer(AsyncResponse asyncResponse, CreateConsumerRequest request,
                                        ConcurrentOpenHashSet<Integer> partitionIndexes, String subscriptionName) {
        List<CreateConsumerResponse.CreateConsumerResult> createConsumerResults = new ArrayList<>();

        for (int partitionIndex : partitionIndexes.values()) {
            String partitionedTopicName = topicName.getPartition(partitionIndex).toString();
            // Subscribe for each partition this broker owns
            pulsar().getBrokerService().getTopic(partitionedTopicName, false).thenAccept(t -> {
                if (!t.isPresent()) {
                    // process error, topic not owned by the broker anymore
                    if (log.isDebugEnabled()) {
                        log.warn("Trying to add rest consumer to subscription: {} on topic {} but topic is not found",
                                subscriptionName, partitionedTopicName);
                    }
                    createConsumerResults.add(new CreateConsumerResponse.CreateConsumerResult("", -1));
                } else {
                    Subscription subscription = t.get().getSubscription(subscriptionName);
                    SubType subType = subscription.getType();
                    // Generate a key from partition topic name and subscription name to keep track of
                    // all rest subscription for this topic.
                    String key = partitionedTopicName + "_" + subscriptionName;
                    if (subType == SubType.Exclusive) {
                        synchronized (this) {
                            if (!subscriptions.containsKey(key)) {
                                // Succeed
                                String consumerId = subscriptionName + UUID.randomUUID();
                                subscriptions.computeIfAbsent(key, k-> new ConcurrentOpenHashMap<>());//.add(consumerId);
                                createConsumerResults.add(new CreateConsumerResponse.CreateConsumerResult(consumerId, partitionIndex));
                            } else {
                                // Other consumer already connect for exclusive subscription.
                                createConsumerResults.add(new CreateConsumerResponse.CreateConsumerResult("", -1));
                            }
                        }
                    } else if (subType == SubType.Shared) {
                        subscriptions.computeIfAbsent(key, k-> new ConcurrentOpenHashMap<>());//.add(subscriptionName + UUID.randomUUID());
                        createConsumerResults.add(new CreateConsumerResponse.CreateConsumerResult(subscriptionName + UUID.randomUUID(), partitionIndex));
                    } else {
                        if (log.isDebugEnabled()) {
                            log.warn("Trying to add rest consumer to subscription: {} on topic {} with subscription type {} is not supported",
                                    subscriptionName, partitionedTopicName, subscription.getTypeString());
                        }
                        createConsumerResults.add(new CreateConsumerResponse.CreateConsumerResult("", -1));
                    }
                }
            }).exceptionally(e -> {
                if (log.isDebugEnabled()) {
                    log.warn("Topic {} not owned by broker {} anymore, rest create consumer request fail on this topic",
                            partitionedTopicName, pulsar().getBrokerServiceUrl());
                }
                createConsumerResults.add(new CreateConsumerResponse.CreateConsumerResult("", -1));
                return null;
            });

        }
        asyncResponse.resume(new CreateConsumerResponse(createConsumerResults, pulsar().getBrokerServiceUrl()));
    }
//
//    private CompletableFuture<Void> internalCreateConsumerOnSinglePartition() {
//        return null;
//    }

    protected ConsumeMessagesResponse consumeMessages(AsyncResponse asyncResponse, ConsumeMessagesRequest request,
                                   String consumerId) {
        //internalConsumerMessages();
        return null;
    }

    private ConsumeMessagesResponse internalConsumerMessages(ConsumeMessagesRequest request, String subscriptionName) {
        //If non partitioned topic or single partition of partitioned topic, read message from single partition
        //If multiple partition topic, start from random partition this broker own.

        List<ConsumeMessagesResponse.ConsumeMessagesResult> messages = new ArrayList<>();
        long startEpoch = System.currentTimeMillis();
        int messagesRead = 0, byteRead = 0;
        List<Integer> owningPartitions = owningTopics.get(topicName.getPartitionedTopicName()).values();
        int startingIndex = random.nextInt(owningPartitions.size());
        while (System.currentTimeMillis() - startEpoch < request.getTimeout() && messagesRead < request.getMaxMessage()
        && byteRead < request.getMaxByte()) {
            int entryToRead = request.getMaxMessage() - messagesRead;
            int partitionsToRead = owningPartitions.size() - startingIndex % owningPartitions.size();
            List<CompletableFuture<List<Entry>>> futures = new ArrayList<>();
            for (int index = startingIndex % owningPartitions.size(); index < owningPartitions.size(); index++, startingIndex++) {
                futures.add(internalConsumeMessageFromSinglePartition(topicName.getPartition(owningPartitions.get(index)).toString(), subscriptionName,
                        entryToRead / partitionsToRead + 1));
            }
            FutureUtil.waitForAll(futures);
            for (int index = 0; index < futures.size() && messagesRead < request.getMaxMessage()
                    && byteRead < request.getMaxByte(); index++) {
                try {
                    List<Entry> entries = futures.get(index).get();
                    for (int i = 0; i < entries.size(); i++) {
                        Entry entry = entries.get(i);
                        ByteBuf metadataAndPayload = entry.getDataBuffer();
                        PulsarApi.MessageMetadata messageMetadata = Commands.parseMessageMetadata(metadataAndPayload);
                        int messageSize = metadataAndPayload.readableBytes();
                        ConsumeMessagesResponse.ConsumeMessagesResult consumeMessagesResult = new ConsumeMessagesResponse.ConsumeMessagesResult();
                        consumeMessagesResult.setEventTime(messageMetadata.getEventTime());
                        //consumeMessagesResult.setPartition(topicName.getPartition(owningPartitions.get());
                        consumeMessagesResult.setLedgerId(entry.getLedgerId());
                        consumeMessagesResult.setEntryId(entry.getEntryId());
                        consumeMessagesResult.setPartition(0);
                        consumeMessagesResult.setProperties(messageMetadata.getPropertiesList().toString());
                        //consumeMessagesResult.setKey();
                        messages.add(consumeMessagesResult);
                        messagesRead++;
                        byteRead += messageSize;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    private CompletableFuture<List<Entry>> internalConsumeMessageFromSinglePartition(String topicName, String subscriptionName, int numberOfEntryToRead) {
        CompletableFuture<List<Entry>> readFuture = new CompletableFuture<>();
        pulsar().getBrokerService().getTopic(topicName, false).thenAccept(t -> {
            if (!t.isPresent()) {
                // process error, topic not owned by the broker anymore
                if (log.isDebugEnabled()) {
                    log.warn("Trying to rest consume message on subscription: {} for topic {} but topic is not found",
                            subscriptionName, topicName);
                }
                readFuture.complete(ImmutableList.of());
            } else {
                Subscription subscription = t.get().getSubscription(subscriptionName);
                if (null == subscription) {
                    if (log.isDebugEnabled()) {
                        log.warn("Trying to rest consume message on subscription: {} for topic {} but subscription is not found",
                                subscriptionName, topicName);
                    }
                    readFuture.complete(ImmutableList.of());
                } else {
                    if (subscription instanceof PersistentSubscription) {
                        // synchronize on the subscription so multiple read won't mess up cursor.
                        synchronized (subscription) {
                            // Persistent subscription.
                            ((PersistentSubscription) subscription).getCursor().asyncReadEntries(numberOfEntryToRead,
                                    new AsyncCallbacks.ReadEntriesCallback() {
                                        @Override
                                        public void readEntriesComplete(List<Entry> entries, Object ctx) {
                                            readFuture.complete(entries);
                                        }

                                        @Override
                                        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                                            readFuture.complete(ImmutableList.of());
                                        }

                                    }, null);
                        }
                    } else {
                        // Non-persistent subscription can't be supported yet as message published to non-persistent topic need to
                        // be immediately deliver to consumer connected, and Rest consumers can't do that.
                        readFuture.complete(ImmutableList.of());
                    }
                }
            }
        });
        return readFuture;
    }

}
