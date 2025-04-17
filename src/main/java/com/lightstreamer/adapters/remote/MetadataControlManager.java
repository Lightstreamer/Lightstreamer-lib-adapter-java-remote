/*
 * Copyright (C) Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lightstreamer.adapters.remote;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.lightstreamer.log.Logger;

class MetadataControlManager {

    private ConcurrentHashMap<String, CompletableFuture<?>> pendingControlRequests = new ConcurrentHashMap<>();

    private final Logger _log;

    private static AtomicLong requestProg = new AtomicLong(MetadataProviderProtocol.FIRST_REMOTE_REQUEST_ID);
    
    public MetadataControlManager(Logger _log) {
        this._log = _log;
    }

    private String nextRequestId() {
        return Long.toHexString(requestProg.getAndIncrement());
    }
    
    public MetadataControlData prepareForceSessionTermination(String sessionID) {
        MetadataControlData controlData = new MetadataControlData();
        controlData.future = new CompletableFuture<Void>();
        controlData.requestID = nextRequestId();
        try {
            controlData.request = MetadataProviderProtocol.writeForceSessionTermination(sessionID);
        } catch (RemotingException e) {
            controlData.request = null;
            controlData.future.completeExceptionally(e);
            return controlData;
        }
        pendingControlRequests.put(controlData.requestID, controlData.future);
        return controlData;
    }

    public MetadataControlData prepareForceSessionTermination(String sessionID, int causeCode, String causeMessage) {
        MetadataControlData controlData = new MetadataControlData();
        controlData.future = new CompletableFuture<Void>();
        controlData.requestID = nextRequestId();
        try {
            controlData.request = MetadataProviderProtocol.writeForceSessionTermination(sessionID, causeCode, causeMessage);
        } catch (RemotingException e) {
            controlData.request = null;
            controlData.future.completeExceptionally(e);
            return controlData;
        }
        pendingControlRequests.put(controlData.requestID, controlData.future);
        return controlData;
    }

    public MetadataControlData prepareForceUnsubscription(String sessionID, int winIndex) {
        MetadataControlData controlData = new MetadataControlData();
        controlData.future = new CompletableFuture<Boolean>();
        controlData.requestID = nextRequestId();
        try {
            controlData.request = MetadataProviderProtocol.writeForceUnsubscription(sessionID, winIndex);
        } catch (RemotingException e) {
            controlData.request = null;
            controlData.future.completeExceptionally(e);
            return controlData;
        }
        pendingControlRequests.put(controlData.requestID, controlData.future);
        return controlData;
    }

    public <T> void onResponse(String requestID, T outcome) {
        CompletableFuture<?> future = pendingControlRequests.remove(requestID);
        if (future != null) {
            ((CompletableFuture<T>) future).complete(outcome);
        } else {
            _log.warn("Received response with unexpected request ID: " + requestID);
        }
    }

    public void onErrorResponse(String requestID, Exception exc) {
        CompletableFuture<?> future = pendingControlRequests.remove(requestID);
        if (future != null) {
            future.completeExceptionally(exc);
        } else {
            _log.warn("Received response with unexpected request ID: " + requestID);
        }
    }

}
