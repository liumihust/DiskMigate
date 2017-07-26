/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azure;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.security.Constants;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.azure.WasbRemoteCallHelper.REMOTE_CALL_SUCCESS_CODE;

/**
 * Class implementing WasbAuthorizerInterface using a remote
 * service that implements the authorization operation. This
 * class expects the url of the remote service to be passed
 * via config.
 */
public class RemoteWasbAuthorizerImpl implements WasbAuthorizerInterface {

  public static final Logger LOG = LoggerFactory
      .getLogger(RemoteWasbAuthorizerImpl.class);
  private static final ObjectReader RESPONSE_READER = new ObjectMapper()
      .readerFor(RemoteWasbAuthorizerResponse.class);

  /**
   * Configuration parameter name expected in the Configuration object to
   * provide the urls of the remote service instances. {@value}
   */
  public static final String KEY_REMOTE_AUTH_SERVICE_URLS =
      "fs.azure.authorization.remote.service.urls";
  /**
   * Authorization operation OP name in the remote service {@value}
   */
  private static final String CHECK_AUTHORIZATION_OP = "CHECK_AUTHORIZATION";
  /**
   * Query parameter specifying the access operation type. {@value}
   */
  private static final String ACCESS_OPERATION_QUERY_PARAM_NAME =
      "operation_type";
  /**
   * Query parameter specifying the wasb absolute path. {@value}
   */
  private static final String WASB_ABSOLUTE_PATH_QUERY_PARAM_NAME =
      "wasb_absolute_path";
  /**
   *  Query parameter name for sending owner of the specific resource {@value}
   */
  private static final String WASB_RESOURCE_OWNER_QUERY_PARAM_NAME =
      "wasb_resource_owner";

  /**
   * Authorization Remote http client retry policy enabled configuration key. {@value}
   */
  private static final String AUTHORIZER_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY =
      "fs.azure.authorizer.http.retry.policy.enabled";

  /**
   * Authorization Remote http client retry policy spec. {@value}
   */
  private static final String AUTHORIZER_HTTP_CLIENT_RETRY_POLICY_SPEC_SPEC =
      "fs.azure.authorizer.http.retry.policy.spec";

  /**
   * Authorization Remote http client retry policy spec default value. {@value}
   */
  private static final String AUTHORIZER_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT =
      "1000,3,10000,2";

  private WasbRemoteCallHelper remoteCallHelper = null;
  private boolean isKerberosSupportEnabled;
  private RetryPolicy retryPolicy;
  private String[] commaSeparatedUrls = null;

  @VisibleForTesting public void updateWasbRemoteCallHelper(
      WasbRemoteCallHelper helper) {
    this.remoteCallHelper = helper;
  }

  @Override
  public void init(Configuration conf)
      throws WasbAuthorizationException, IOException {
    LOG.debug("Initializing RemoteWasbAuthorizerImpl instance");
    this.isKerberosSupportEnabled =
        conf.getBoolean(Constants.AZURE_KERBEROS_SUPPORT_PROPERTY_NAME, false);
    this.commaSeparatedUrls =
        conf.getTrimmedStrings(KEY_REMOTE_AUTH_SERVICE_URLS);
    if (this.commaSeparatedUrls == null
        || this.commaSeparatedUrls.length <= 0) {
      throw new IOException(KEY_REMOTE_AUTH_SERVICE_URLS + " config not set"
          + " in configuration.");
    }
    this.retryPolicy = RetryUtils.getMultipleLinearRandomRetry(conf,
        AUTHORIZER_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY, true,
        AUTHORIZER_HTTP_CLIENT_RETRY_POLICY_SPEC_SPEC,
        AUTHORIZER_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT);
    if (isKerberosSupportEnabled && UserGroupInformation.isSecurityEnabled()) {
      this.remoteCallHelper = new SecureWasbRemoteCallHelper(retryPolicy, false);
    } else {
      this.remoteCallHelper = new WasbRemoteCallHelper(retryPolicy);
    }
  }

  @Override
  public boolean authorize(String wasbAbsolutePath, String accessType, String resourceOwner)
      throws WasbAuthorizationException, IOException {

    try {
        /* Make an exception for the internal -RenamePending files */
      final URIBuilder uriBuilder = new URIBuilder();
      uriBuilder.setPath("/" + CHECK_AUTHORIZATION_OP);
      uriBuilder
          .addParameter(WASB_ABSOLUTE_PATH_QUERY_PARAM_NAME, wasbAbsolutePath);
      uriBuilder.addParameter(ACCESS_OPERATION_QUERY_PARAM_NAME, accessType);
      if (resourceOwner != null && StringUtils.isNotEmpty(resourceOwner)) {
        uriBuilder.addParameter(WASB_RESOURCE_OWNER_QUERY_PARAM_NAME,
            resourceOwner);
      }

      String responseBody = remoteCallHelper
          .makeRemoteRequest(commaSeparatedUrls, uriBuilder.getPath(),
              uriBuilder.getQueryParams(), HttpGet.METHOD_NAME);

      RemoteWasbAuthorizerResponse authorizerResponse = RESPONSE_READER
          .readValue(responseBody);

      if (authorizerResponse == null) {
        throw new WasbAuthorizationException(
            "RemoteWasbAuthorizerResponse object null from remote call");
      } else if (authorizerResponse.getResponseCode()
          == REMOTE_CALL_SUCCESS_CODE) {
        return authorizerResponse.getAuthorizationResult();
      } else {
        throw new WasbAuthorizationException(
            "Remote authorization" + " service encountered an error "
                + authorizerResponse.getResponseMessage());
      }
    } catch (WasbRemoteCallException | JsonParseException | JsonMappingException ex) {
      throw new WasbAuthorizationException(ex);
    }
  }
}

/**
 * POJO representing the response expected from a remote
 * authorization service.
 * The remote service is expected to return the authorization
 * response in the following JSON format
 * {
 *   "responseCode" : 0 or non-zero <int>,
 *   "responseMessage" : relevant message of failure <String>
 *   "authorizationResult" : authorization result <boolean>
 *   true - if auhorization allowed
 *   false - otherwise.
 * }
 */
class RemoteWasbAuthorizerResponse {

  private int responseCode;
  private boolean authorizationResult;
  private String responseMessage;

  public int getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(int responseCode) {
    this.responseCode = responseCode;
  }

  public boolean getAuthorizationResult() {
    return authorizationResult;
  }

  public void setAuthorizationResult(boolean authorizationResult) {
    this.authorizationResult = authorizationResult;
  }

  public String getResponseMessage() {
    return responseMessage;
  }

  public void setResponseMessage(String message) {
    this.responseMessage = message;
  }
}