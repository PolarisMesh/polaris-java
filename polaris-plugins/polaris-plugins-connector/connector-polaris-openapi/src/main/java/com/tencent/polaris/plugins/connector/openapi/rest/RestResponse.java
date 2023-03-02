/*
 * Tencent is pleased to support the open source community by making Polaris available.
 *
 * Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.polaris.plugins.connector.openapi.rest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

/**
 * @author fabian4 2023-02-28
 */
public class RestResponse<T> {

    private final RestClientException exception;

    private final ResponseEntity<T> responseEntity;

    private final int rawStatusCode;

    private final String statusText;

    private RestResponse(RestClientException exception,
                         ResponseEntity<T> responseEntity, int rawStatusCode, String statusText) {
        this.exception = exception;
        this.responseEntity = responseEntity;
        this.rawStatusCode = rawStatusCode;
        this.statusText = statusText;
    }

    public static RestResponse<String> withNormalResponse(ResponseEntity<String> responseEntity) {
        return new RestResponse<>(null, responseEntity,
                responseEntity.getStatusCode().value(), responseEntity.getStatusCode().getReasonPhrase());
    }

    public static RestResponse<String> withRestClientException(RestClientException restClientException) {
        if (restClientException instanceof RestClientResponseException) {
            //存在消息返回，只是前面没有出错
            RestClientResponseException restClientResponseException = (RestClientResponseException) restClientException;
            ResponseEntity<String> responseEntity = new ResponseEntity<>(restClientResponseException.getResponseBodyAsString(), HttpStatus.BAD_REQUEST);
            return new RestResponse<>(null, responseEntity,
                    restClientResponseException.getRawStatusCode(), restClientResponseException.getStatusText());
        } else {
            return new RestResponse<>(restClientException, null, 0, "");
        }
    }

    public boolean hasServerError() {
        return null != exception;
    }

    public boolean hasTextError() {
        return null == exception && null == responseEntity;
    }

    public RestClientException getException() {
        return exception;
    }

    public ResponseEntity<T> getResponseEntity() {
        return responseEntity;
    }

    @Override
    public String toString() {
        return "RestResponse{" +
                "exception=" + exception +
                ", responseEntity=" + responseEntity +
                ", rawStatusCode=" + rawStatusCode +
                ", statusText='" + statusText + '\'' +
                '}';
    }
}

