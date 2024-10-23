/*
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
package io.tabular.iceberg.connect.channel;

import static java.util.stream.Collectors.groupingBy;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitState {
  private static final Logger LOG = LoggerFactory.getLogger(CommitState.class);

  private final List<Envelope> commitBuffer = new LinkedList<>();
  private final List<CommitReadyPayload> readyBuffer = new LinkedList<>();
  private long startTime;
  private UUID currentCommitId;
  private final IcebergSinkConfig config;

  public CommitState(IcebergSinkConfig config) {
    this.config = config;
  }

  public void addResponse(Envelope envelope) {
    commitBuffer.add(envelope);
    if (!isCommitInProgress()) {
      LOG.warn(
          "Received commit response with commit-id={} when no commit in progress, this can happen during recovery",
          ((CommitResponsePayload) envelope.event().payload()).commitId());
    }
  }

  public void addReady(Envelope envelope) {
    readyBuffer.add((CommitReadyPayload) envelope.event().payload());
    if (!isCommitInProgress()) {
      LOG.warn(
          "Received commit ready for commit-id={} when no commit in progress, this can happen during recovery",
          ((CommitReadyPayload) envelope.event().payload()).commitId());
    }
  }

  public UUID currentCommitId() {
    return currentCommitId;
  }

  public boolean isCommitInProgress() {
    return currentCommitId != null;
  }

  public boolean isCommitIntervalReached() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    return (!isCommitInProgress()
        && System.currentTimeMillis() - startTime >= config.commitIntervalMs());
  }

  public void startNewCommit() {
    currentCommitId = UUID.randomUUID();
    startTime = System.currentTimeMillis();
  }

  public void endCurrentCommit() {
    readyBuffer.clear();
    currentCommitId = null;
  }

  public void clearResponses() {
    LOG.info("Clearing commit buffer");
    commitBuffer.clear();
  }

  public boolean isCommitTimedOut() {
    if (!isCommitInProgress()) {
      return false;
    }

    if (System.currentTimeMillis() - startTime > config.commitTimeoutMs()) {
      LOG.info("Commit timeout reached");
      return true;
    }
    return false;
  }

  public boolean isCommitReady(int expectedPartitionCount) {
    if (!isCommitInProgress()) {
      return false;
    }

    int receivedPartitionCount =
        readyBuffer.stream()
            .filter(payload -> payload.commitId().equals(currentCommitId))
            .mapToInt(payload -> payload.assignments().size())
            .sum();

    if (receivedPartitionCount >= expectedPartitionCount) {
      LOG.info(
          "Commit {} ready, received responses for all {} partitions",
          currentCommitId,
          receivedPartitionCount);
      return true;
    }

    LOG.info(
        "Commit {} not ready, received responses for {} of {} partitions, waiting for more",
        currentCommitId,
        receivedPartitionCount,
        expectedPartitionCount);

    return false;
  }

  public List<List<Envelope>> tokenize(List<Envelope> list) {
    List<List<Envelope>> tokenized = Lists.newArrayList();
    List<Envelope> tempList = new LinkedList<>();
    AtomicBoolean lastHasEqualityDeletes = new AtomicBoolean(true);
    list.forEach(
        envelope -> {
          boolean checkEqualityDeletes =
              ((CommitResponsePayload) envelope.event().payload())
                  .deleteFiles().stream()
                      .anyMatch(x -> x.content() == FileContent.EQUALITY_DELETES);
          if (checkEqualityDeletes && !tempList.isEmpty()) {
            if (!lastHasEqualityDeletes.get()) {
              tokenized.add(List.copyOf(tempList));
              tempList.clear();
            }
          }
          tempList.add(envelope);
          lastHasEqualityDeletes.set(checkEqualityDeletes);
        });
    tokenized.add(tempList);
    return tokenized;
  }

  public Map<TableIdentifier, List<List<Envelope>>> tableCommitMap() {
    Map<TableIdentifier, List<Envelope>> tempCommitMap =
        commitBuffer.stream()
            .collect(
                groupingBy(
                    envelope ->
                        ((CommitResponsePayload) envelope.event().payload())
                            .tableName()
                            .toIdentifier()));

    Map<TableIdentifier, List<List<Envelope>>> commitMap = Maps.newHashMap();
    tempCommitMap.forEach((k, v) -> commitMap.put(k, tokenize(v)));
    return commitMap;
  }

  public Long vtts(boolean partialCommit) {
    boolean validVtts =
        !partialCommit
            && readyBuffer.stream()
                .flatMap(event -> event.assignments().stream())
                .allMatch(offset -> offset.timestamp() != null);

    Long result;
    if (validVtts) {
      result =
          readyBuffer.stream()
              .flatMap(event -> event.assignments().stream())
              .mapToLong(TopicPartitionOffset::timestamp)
              .min()
              .getAsLong();
    } else {
      result = null;
    }
    return result;
  }
}
