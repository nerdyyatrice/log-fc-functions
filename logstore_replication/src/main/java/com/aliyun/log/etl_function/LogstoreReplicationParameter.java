package com.aliyun.log.etl_function;

import com.aliyun.fc.runtime.FunctionComputeLogger;
import com.aliyun.log.etl_function.common.Consts;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class LogstoreReplicationParameter {

    private FunctionComputeLogger logger;
    private String targetLogEndpoint;
    private String targetLogProjectName;
    private String targetLogLogstoreName;

    public LogstoreReplicationParameter(FunctionComputeLogger logger) {
    }

    boolean parseFromJsonObject(JSONObject jsonObj) {
        try {
            JSONObject targetObj = jsonObj.getJSONObject(Consts.EVENT_TARGET_FIELD_NAME);
            this.targetLogEndpoint = targetObj.getString(Consts.EVENT_LOG_ENDPOINT_FIELD_NAME);
            this.targetLogProjectName = targetObj.getString(Consts.EVENT_LOG_PROJECT_FIELD_NAME);
            this.targetLogLogstoreName = targetObj.getString(Consts.EVENT_LOG_LOGSTORE_FIELD_NAME);
            return true;
        } catch (JSONException e) {
            logger.error("invalid function parameter, exception: " + e.getMessage());
            return false;
        }
    }

    public String getTargetLogEndpoint() {
        return targetLogEndpoint;
    }

    public String getTargetLogProjectName() {
        return targetLogProjectName;
    }

    public String getTargetLogLogstoreName() {
        return targetLogLogstoreName;
    }
}
