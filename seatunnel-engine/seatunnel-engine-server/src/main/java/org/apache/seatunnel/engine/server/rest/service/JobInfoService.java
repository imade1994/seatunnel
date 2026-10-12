/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.config.sql.SqlConfigBuilder;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;
import org.apache.seatunnel.engine.server.operation.GetJobMetricsOperation;
import org.apache.seatunnel.engine.server.persistence.FinishedJobHistoryReader;
import org.apache.seatunnel.engine.server.rest.ConfigFormat;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.server.rest.RestConstant.CONFIG_FORMAT;

@Slf4j
public class JobInfoService extends BaseService {

    public JobInfoService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonObject getJobInfoJson(Long jobId) {
        IMap<Object, Object> jobInfoMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        JobInfo jobInfo = (JobInfo) jobInfoMap.get(jobId);

        IMap<Object, Object> finishedJobStateMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_STATE);
        JobState finishedJobState = (JobState) finishedJobStateMap.get(jobId);

        if (jobInfo != null) {
            return convertToJson(jobInfo, jobId);
        } else if (finishedJobState != null) {
            JobMetrics finishedJobMetrics =
                    (JobMetrics)
                            nodeEngine
                                    .getHazelcastInstance()
                                    .getMap(Constant.IMAP_FINISHED_JOB_METRICS)
                                    .get(jobId);
            JobDAGInfo finishedJobDAGInfo =
                    (JobDAGInfo)
                            nodeEngine
                                    .getHazelcastInstance()
                                    .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO)
                                    .get(jobId);
            return getJobInfoJson(
                    finishedJobState,
                    finishedJobMetrics == null
                            ? JobMetrics.empty().toJsonString()
                            : finishedJobMetrics.toJsonString(),
                    finishedJobDAGInfo);
        } else {
            return new JsonObject().add(RestConstant.JOB_ID, jobId.toString());
        }
    }

    public JsonArray getJobsByStateJson(String state) {
        List<JobState> finishedJobs = loadFinishedJobStates();
        Map<Long, JobDAGInfo> finishedJobDAGInfo = loadFinishedJobDagInfos(finishedJobs);

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);

        return finishedJobs.stream()
                .filter(
                        jobState -> {
                            if (state.isEmpty()) {
                                return true;
                            }
                            return jobState.getJobStatus().name().equals(state.toUpperCase());
                        })
                .sorted(Comparator.comparing(JobState::getFinishTime, Comparator.reverseOrder()))
                .map(
                        jobState -> {
                            Long jobId = jobState.getJobId();
                            String jobMetrics;
                            if (seaTunnelServer == null) {
                                jobMetrics =
                                        (String)
                                                NodeEngineUtil.sendOperationToMasterNode(
                                                                nodeEngine,
                                                                new GetJobMetricsOperation(jobId))
                                                        .join();
                            } else {
                                jobMetrics =
                                        seaTunnelServer
                                                .getCoordinatorService()
                                                .getJobMetrics(jobId)
                                                .toJsonString();
                            }
                            return getJobInfoJson(
                                    jobState, jobMetrics, finishedJobDAGInfo.get(jobId));
                        })
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }

    /**
     * 已完成作业的状态列表。历史 map 启动时被跳过加载，而 Hazelcast 的 {@code IMap#values()} 不读穿、 只扫内存，所以这里直查存储；存储不可用时回退到内存
     * map（那样只含本次启动之后完成的作业）。
     */
    private List<JobState> loadFinishedJobStates() {
        FinishedJobHistoryReader reader = historyReader(Constant.IMAP_FINISHED_JOB_STATE);
        if (reader == null) {
            return nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_STATE)
                    .values().stream()
                    .map(JobState.class::cast)
                    .collect(Collectors.toList());
        }
        return reader.loadLatest(reader.getQueryLimit()).values().stream()
                .map(JobState.class::cast)
                .collect(Collectors.toList());
    }

    /** 与 {@link #loadFinishedJobStates()} 同理，批量取 DAG 信息（逐条 {@code get} 会开 N 次连接）。 */
    private Map<Long, JobDAGInfo> loadFinishedJobDagInfos(List<JobState> finishedJobs) {
        List<Long> jobIds =
                finishedJobs.stream().map(JobState::getJobId).collect(Collectors.toList());
        if (jobIds.isEmpty()) {
            return Collections.emptyMap();
        }
        FinishedJobHistoryReader reader = historyReader(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);
        if (reader == null) {
            IMap<Long, JobDAGInfo> dagInfoMap =
                    nodeEngine
                            .getHazelcastInstance()
                            .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);
            return dagInfoMap.getAll(new HashSet<>(jobIds));
        }
        Map<Long, JobDAGInfo> result = new HashMap<>(jobIds.size());
        reader.loadByIds(jobIds)
                .forEach((key, value) -> result.put((Long) key, (JobDAGInfo) value));
        return result;
    }

    private FinishedJobHistoryReader historyReader(String mapName) {
        return FinishedJobHistoryReader.forMap(nodeEngine.getHazelcastInstance(), mapName);
    }

    public JsonArray getRunningJobsJson() {
        IMap<Long, JobInfo> values =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        return values.entrySet().stream()
                .sorted(
                        Comparator.comparing(
                                entry -> entry.getValue().getInitializationTimestamp(),
                                Comparator.reverseOrder()))
                .map(jobInfoEntry -> convertToJson(jobInfoEntry.getValue(), jobInfoEntry.getKey()))
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }

    public JsonObject stopJob(byte[] requestBody) {
        Map<String, Object> map = JsonUtils.toMap(requestHandle(requestBody));
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        handleStopJob(map, seaTunnelServer, nodeEngine.getNode());
        return new JsonObject().add(RestConstant.JOB_ID, map.get(RestConstant.JOB_ID).toString());
    }

    public JsonArray stopJobs(byte[] requestBody) {
        JsonArray jsonResponse = new JsonArray();
        List<Map> jobList = JsonUtils.toList(requestHandle(requestBody).toString(), Map.class);

        jobList.forEach(
                job -> {
                    handleStopJob(job, getSeaTunnelServer(false), nodeEngine.getNode());
                    jsonResponse.add(
                            new JsonObject()
                                    .add(RestConstant.JOB_ID, (Long) job.get(RestConstant.JOB_ID)));
                });

        return jsonResponse;
    }

    public JsonObject submitJob(Map<String, String> requestParams, byte[] requestBody) {

        if (Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT))
                && requestParams.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("Please provide jobId when start with save point.");
        }
        Config config;
        ConfigFormat configFormat = ConfigFormat.fromString(requestParams.get(CONFIG_FORMAT));
        switch (configFormat) {
            case HOCON:
                config = ConfigFactory.parseString(new String(requestBody, StandardCharsets.UTF_8));
                break;
            case SQL:
                config = SqlConfigBuilder.of(new String(requestBody, StandardCharsets.UTF_8));
                break;
            case JSON:
            default:
                config = RestUtil.buildConfig(requestHandle(requestBody), false);
                break;
        }
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        return submitJobInternal(config, requestParams, seaTunnelServer, nodeEngine.getNode());
    }

    public JsonObject submitJob(Map<String, String> requestParams, Config config) {
        if (Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT))
                && requestParams.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("Please provide jobId when start with save point.");
        }
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        return submitJobInternal(config, requestParams, seaTunnelServer, nodeEngine.getNode());
    }

    public JsonArray submitJobs(byte[] requestBody) {
        List<Tuple2<Map<String, String>, Config>> configTuples =
                RestUtil.buildConfigList(requestHandle(requestBody), false);

        return configTuples.stream()
                .map(
                        tuple -> {
                            String urlParams = mapToUrlParams(tuple._1);
                            Map<String, String> requestParams = new HashMap<>();
                            RestUtil.buildRequestParams(requestParams, urlParams);
                            SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
                            return submitJobInternal(
                                    tuple._2, requestParams, seaTunnelServer, nodeEngine.getNode());
                        })
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }
}
