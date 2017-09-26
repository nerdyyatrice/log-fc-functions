package com.aliyun.log.etl_function;

import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.StreamRequestHandler;

import java.io.*;
import java.util.List;

import com.aliyun.fc.runtime.*;
import com.aliyun.log.etl_function.common.FunctionEvent;
import com.aliyun.log.etl_function.common.FunctionResponse;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.request.PutLogsRequest;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;

public class LogstoreReplication implements StreamRequestHandler {

    private final static int MAX_RETRY_TIMES = 10;
    private final static int RETRY_SLEEP_MILLIS = 50;
    private final static Boolean IGNORE_FAIL = false;

    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        FunctionComputeLogger logger = context.getLogger();
        FunctionEvent event = new FunctionEvent(logger);
        if (!event.parseFromInputStream(inputStream)) {
            throw new IOException("read input stream fail");
        }
        LogstoreReplicationParameter parameter = new LogstoreReplicationParameter();
        if (!parameter.parseFromJsonObject(event.getParameterJsonObject())) {
            throw new IOException("parse input stream into json event fail");
        }

        Credentials credentials = context.getExecutionCredentials();
        String accessKeyId = credentials.getAccessKeyId();
        String accessKeySecret = credentials.getAccessKeySecret();
        String securityToken = credentials.getSecurityToken();

        String logEndpoint = event.getLogEndpoint();
        String logProjectName = event.getLogProjectName();
        String logLogstoreName = event.getLogLogstoreName();
        int logShardId = event.getLogShardId();
        String logBeginCursor = event.getLogBeginCursor();
        String logEndCurosr = event.getLogEndCursor();

        String targetEndpoint = parameter.getTargetLogEndpoint();
        String targetProjectName = parameter.getTargetLogProjectName();
        String targetLogstoreName = parameter.getTargetLogLogstoreName();

        Client logClient = new Client(logEndpoint, accessKeyId, accessKeySecret);
        logClient.SetSecurityToken(securityToken);

        Client targetClient = new Client(targetEndpoint, accessKeyId, accessKeySecret);
        targetClient.SetSecurityToken(securityToken);

        String cursor = logBeginCursor;
        FunctionResponse response = new FunctionResponse();

        while (!cursor.equals(event.getLogEndCursor())) {
            List<LogGroupData> logGroupDataList = null;
            String nextCursor = "";
            int rawSize = 0;
            int retryTime = 0;
            while (true) {
                ++retryTime;
                try {
                    BatchGetLogResponse logDataRes = logClient.BatchGetLog(logProjectName, logLogstoreName, logShardId,
                            3, cursor, logEndCurosr);
                    logGroupDataList = logDataRes.GetLogGroups();
                    rawSize = logDataRes.GetRawSize();
                    nextCursor = logDataRes.GetNextCursor();
                    logger.info("BatchGetLog success, project_name: " + logProjectName + ", job_name: " + event.getJobName()
                            + ", task_id: " + event.getTaskId() + ", cursor: " + cursor + ", logGroup count: " + logGroupDataList.size());
                    break;
                } catch (LogException e) {
                    if (retryTime >= MAX_RETRY_TIMES) {
                        if (IGNORE_FAIL) {
                            logger.error("BatchGetLog fail and ignore the fail, project_name: " + logProjectName
                                    + ", job_name: " + event.getJobName() + ", task_id: " + event.getTaskId() +
                                    "retry_time: " + retryTime + ", error_code: " + e.GetErrorCode() +
                                    ", error_message: " + e.GetErrorMessage() + ", request_id: " + e.GetRequestId());
                            break;
                        } else {
                            throw new IOException("BatchGetLog fail, retry_time: " + retryTime + ", error_code: " + e.GetErrorCode()
                                    + ", error_message: " + e.GetErrorMessage() + ", request_id: " + e.GetRequestId());
                        }
                    }
                        logger.warn("BatchGetLog fail, project_name: " + logProjectName + ", job_name: " + event.getJobName() + ", task_id: "
                                + event.getTaskId() + ", retry_time: " + retryTime + ", error_code: " + e.GetErrorCode()
                                + ", error_message: " + e.GetErrorMessage() + ", request_id: " + e.GetRequestId());
                    try {
                        Thread.sleep(RETRY_SLEEP_MILLIS);
                    } catch (InterruptedException ie) {
                    }
                }
            }
            response.addIngestBytes(rawSize);
            for (LogGroupData logGroupData : logGroupDataList) {
                FastLogGroup fastLogGroup = logGroupData.GetFastLogGroup();
                byte[] logGroupBytes = fastLogGroup.getBytes();
                response.addIngestLines(fastLogGroup.getLogsCount());
                PutLogsRequest req = new PutLogsRequest(targetProjectName, targetLogstoreName,
                        fastLogGroup.hasTopic() ? fastLogGroup.getTopic() : "",
                        fastLogGroup.hasSource() ? fastLogGroup.getSource() : "",
                        logGroupBytes, null);
                response.addIngestBytes(logGroupBytes.length);
                retryTime = 0;
                while (true) {
                    ++retryTime;
                    try {
                        targetClient.PutLogs(req);
                        response.addShipLines(fastLogGroup.getLogsCount());
                        response.addShipBytes(logGroupBytes.length);
                        logger.info("PutLogs success, project_name: " + logProjectName + ", job_name: " + event.getJobName()
                                + ", task_id: " + event.getTaskId() + ", logcount: " + fastLogGroup.getLogsCount());
                        break;
                    } catch (LogException e) {
                        if (retryTime >= MAX_RETRY_TIMES) {
                            if (IGNORE_FAIL) {
                                logger.error("PutLogs fail and ignore the fail, project_name: " + logProjectName +
                                        ", job_name: " + event.getJobName() + ", task_id: " + event.getTaskId() + ", error_code: "
                                        + e.GetErrorCode() + ", error_message: " + e.GetErrorMessage() + ", request_id: " + e.GetRequestId());
                                break;
                            } else {
                                throw new IOException("PutLogs fail, retryTime: " + retryTime + ", error_code: " + e.GetErrorCode()
                                        + ", error_message: " + e.GetErrorMessage() + ", request_id: " + e.GetRequestId());
                            }
                        }
                        try {
                            Thread.sleep(RETRY_SLEEP_MILLIS);
                        } catch (InterruptedException ie) {
                        }
                        logger.warn( "PutLogs fail, project_name: " + logProjectName + ", job_name: " + event.getJobName()
                                + ", task_id: " + event.getTaskId() + ", retry_time: " + retryTime + ", error_code: "
                                + e.GetErrorCode() + ", error_message: " + e.GetErrorMessage() + ", request_id: " + e.GetRequestId());
                    }
                }
            }
            response.addShipBytes(rawSize);
            cursor = nextCursor;
        }
        outputStream.write(response.toJsonString().getBytes());
    }
}