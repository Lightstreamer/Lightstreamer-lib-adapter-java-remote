/*
 *  Copyright (c) Lightstreamer Srl
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.lightstreamer.adapters.remote;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.StringTokenizer;

class DataProviderProtocol extends BaseProtocol {

    public static final char TYPE_DIFF_ALGORITHMS= 'F';
    public static final char TYPE_ALGORITHM_JSONPATCH= 'J';
    public static final char TYPE_ALGORITHM_DIFFMATCHPATCH= 'M'; // corresponding to the TLCP-diff format

    public static final char SUBTYPE_DATAPROVIDER_EXCEPTION = 'D';
    public static final char SUBTYPE_FAILURE_EXCEPTION = 'F';
    public static final char SUBTYPE_SUBSCRIPTION_EXCEPTION = 'U';

    public static final String METHOD_DATA_INIT = "DPI";
    public static final String METHOD_SUBSCRIBE = "SUB";
    public static final String METHOD_UNSUBSCRIBE = "USB";
    public static final String METHOD_END_OF_SNAPSHOT = "EOS";
    public static final String METHOD_UPDATE_BY_MAP = "UD3";
    public static final String METHOD_DECLARE_FIELD_DIFF_ORDER = "DFD";
    public static final String METHOD_CLEAR_SNAPSHOT = "CLS";

    // ////////////////////////////////////////////////////////////////////////
    // REMOTE INIT

    public static Map<String,String> readInit(String request) throws RemotingException {
        
        StringTokenizer tokenizer = new StringTokenizer(request, "" + SEP);
        String msg = null;
        RemotingException re1 = null;
        
        Map<String,String> parameters = new HashMap<String,String>();

        String typ = null;
        while (tokenizer.hasMoreTokens()) {
            
            String headerName = "";
            String headerValue = "";
            
            typ = tokenizer.nextToken();
            
            String val; // declared here to avoid JVM bug JDK-8067429
            switch (typ.toCharArray()[0]) {

                case TYPE_STRING:
                    val = tokenizer.nextToken();
                    headerName = decodeStringOld(val);
                    
                    break;

                default:
                    msg = "Unknown type '" + typ + "' found while parsing a " + METHOD_DATA_INIT + " request";
                    re1 = new RemotingException(msg);
                    
            }

            typ = tokenizer.nextToken();

            switch (typ.toCharArray()[0]) {

                case TYPE_STRING:
                    val = tokenizer.nextToken();
                    
                    headerValue = decodeStringOld(val);
                 
                    break;

                default:
                    msg = "Unknown type '" + typ + "' found while parsing a " + METHOD_DATA_INIT + " request";
                    re1 = new RemotingException(msg);
                    
            }
            if (re1 != null ) {
                throw re1;
            } else {
                parameters.put(headerName, headerValue);
            }
            
        }

        return parameters;
        
    }

    public static String writeInit(Map<String,String> arguments) throws RemotingException {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_DATA_INIT);

        if (arguments != null) {
            assert (! arguments.isEmpty());
            // protocol version 1.8.1 and above
            Iterator<Map.Entry<String,String>> iter = arguments.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String,String> entry = iter.next();
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeStringOld(entry.getKey()));
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeStringOld(entry.getValue()));
            }
        } else {
            // protocol version 1.8.0
            sb.append(SEP);
            sb.append(TYPE_VOID);
        }

        return sb.toString();
    }

    public static String writeInit(Throwable exception) throws RemotingException {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_DATA_INIT);
        sb.append(SEP);
        sb.append(TYPE_EXCEPTION);
        if (exception instanceof DataProviderException) {
            sb.append(SUBTYPE_DATAPROVIDER_EXCEPTION);
        }
        if (exception instanceof VersionException) {
            // VersionException possible here, but used internally and not specified in the protocol
        }
        sb.append(SEP);
        sb.append(encodeStringOld(exception.getMessage()));

        return sb.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // SUBSCRIBE

    public static SubscribeData readSubscribe(String request) throws RemotingException {
        StringTokenizer tokenizer = new StringTokenizer(request, "" + SEP);

        SubscribeData data = new SubscribeData();

        String typ = null;
        try {
            typ = tokenizer.nextToken();
        } catch (NoSuchElementException e) {
            throw new RemotingException("Token not found while parsing a " + METHOD_SUBSCRIBE + " request");
        }
        switch (typ.toCharArray()[0]) {

            case TYPE_STRING:
                String itemName = tokenizer.nextToken();
                data.itemName = decodeString(itemName);
                break;

            default:
                throw new RemotingException("Unknown type '" + typ + "' found while parsing a " + METHOD_SUBSCRIBE + " request");
        }

        return data;
    }

    public static String writeSubscribe() {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_SUBSCRIBE);
        sb.append(SEP);
        sb.append(TYPE_VOID);

        return sb.toString();
    }

    public static String writeSubscribe(Throwable exception) throws RemotingException {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_SUBSCRIBE);
        sb.append(SEP);
        sb.append(TYPE_EXCEPTION);
        if (exception instanceof SubscriptionException) {
            sb.append(SUBTYPE_SUBSCRIPTION_EXCEPTION);
        }
        if (exception instanceof FailureException) {
            sb.append(SUBTYPE_FAILURE_EXCEPTION);
        }
        sb.append(SEP);
        sb.append(encodeString(exception.getMessage()));

        return sb.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // UNSUBSCRIBE

    public static String readUnsubscribe(String request) throws RemotingException {
        StringTokenizer tokenizer = new StringTokenizer(request, "" + SEP);

        String typ = null;
        try {
            typ = tokenizer.nextToken();
        } catch (NoSuchElementException e) {
            throw new RemotingException("Token not found while parsing a " + METHOD_UNSUBSCRIBE + " request");
        }
        switch (typ.toCharArray()[0]) {

            case TYPE_STRING:
                String val = tokenizer.nextToken();
                return decodeString(val);

            default:
                throw new RemotingException("Unknown type '" + typ + "' found while parsing a " + METHOD_UNSUBSCRIBE + " request");
        }
    }

    public static String writeUnsubscribe() {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_UNSUBSCRIBE);
        sb.append(SEP);
        sb.append(TYPE_VOID);

        return sb.toString();
    }

    public static String writeUnsubscribe(Throwable exception) throws RemotingException {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_UNSUBSCRIBE);
        sb.append(SEP);
        sb.append(TYPE_EXCEPTION);
        if (exception instanceof SubscriptionException) {
            sb.append(SUBTYPE_SUBSCRIPTION_EXCEPTION);
        }
        if (exception instanceof FailureException) {
            sb.append(SUBTYPE_FAILURE_EXCEPTION);
        }
        sb.append(SEP);
        sb.append(encodeString(exception.getMessage()));

        return sb.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // END OF SNAPSHOT

    public static String writeEndOfSnapshot(String itemName, String requestID) throws RemotingException {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_END_OF_SNAPSHOT);
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(encodeString(itemName));
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(requestID);

        return sb.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // UPDATE (String itemName, IndexedItemEvent event, boolean isSnapshot)

    public static String writeUpdateByIndexedEvent(String itemName, String requestID, IndexedItemEvent itemEvent, boolean isSnapshot) throws RemotingException {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_UPDATE_BY_MAP); // since we will write it as a set of key-value pairs
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(encodeString(itemName));
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(requestID);
        sb.append(SEP);
        sb.append(TYPE_BOOLEAN);
        sb.append(SEP);
        sb.append(isSnapshot ? VALUE_TRUE : VALUE_FALSE);

        for (int i = 0; i <= itemEvent.getMaximumIndex(); i++) {
            sb.append(SEP);
            sb.append(TYPE_INT);
            sb.append(SEP);
            sb.append(i);

            sb.append(SEP);
            sb.append(TYPE_STRING);
            sb.append(SEP);
            sb.append(encodeString(itemEvent.getName(i)));

            Object value = itemEvent.getValue(i);
            if (value == null) {
                // with no type information, let's handle it as a string
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeString(null));

            } else if (value instanceof String) {
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeString((String) value));

            } else if (value instanceof byte []) {
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeBytesAsString((byte []) value));

            } else {
                throw new RemotingException("Found value '" + value.toString() + "' of an unsupported type while building a " + METHOD_UPDATE_BY_MAP + " request");
            }
        }

        return sb.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // UPDATE (String itemName, ItemEvent event, boolean isSnapshot)

    public static String writeUpdateByEvent(String itemName, String requestID, ItemEvent itemEvent, boolean isSnapshot) throws RemotingException {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_UPDATE_BY_MAP); // since we will write it as a set of key-value pairs
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(encodeString(itemName));
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(requestID);
        sb.append(SEP);
        sb.append(TYPE_BOOLEAN);
        sb.append(SEP);
        sb.append(isSnapshot ? VALUE_TRUE : VALUE_FALSE);

        Iterator<String> iter = itemEvent.getNames();
        while (iter.hasNext()) {
            String iterValue = iter.next();
            
            sb.append(SEP);
            sb.append(TYPE_STRING);
            sb.append(SEP);
            sb.append(encodeString(iterValue));

            Object value = itemEvent.getValue(iterValue);
            if (value == null) {
                // with no type information, let's handle it as a string
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeString(null));

            } else if (value instanceof String) {
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeString((String) value));

            } else if (value instanceof byte[]) {
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeBytesAsString((byte []) value));

            } else {
                throw new RemotingException("Found value '" + value.toString() + "' of an unsupported type while building a " + METHOD_UPDATE_BY_MAP + " request");
            }
        }

        return sb.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // UPDATE (String itemName, Map event, boolean isSnapshot)

    public static String writeUpdateByMap(String itemName, String requestID, Map<String,?> itemEvent, boolean isSnapshot) throws RemotingException {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_UPDATE_BY_MAP);
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(encodeString(itemName));
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(requestID);
        sb.append(SEP);
        sb.append(TYPE_BOOLEAN);
        sb.append(SEP);
        sb.append(isSnapshot ? VALUE_TRUE : VALUE_FALSE);

        for (String name : itemEvent.keySet()) {
            sb.append(SEP);
            sb.append(TYPE_STRING);
            sb.append(SEP);
            sb.append(encodeString(name));

            Object value = itemEvent.get(name);
            if (value == null) {
                // with no type information, let's handle it as a string
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeString(null));

            } else if (value instanceof String) {
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeString((String) value));

            }  else if (value instanceof byte []) {
                sb.append(SEP);
                sb.append(TYPE_STRING);
                sb.append(SEP);
                sb.append(encodeBytesAsString((byte []) value));

            } else {
                throw new RemotingException("Found value '" + value.toString() + "' of an unsupported type while building a " + METHOD_UPDATE_BY_MAP + " request");
            }
        }

        return sb.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // CLEAR SNAPSHOT

    public static String writeClearSnapshot(String itemName, String requestID) throws RemotingException {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_CLEAR_SNAPSHOT);
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(encodeString(itemName));
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(requestID);

        return sb.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // DECLARE FIELD DIFF ORDER (String itemName, Map algs)

    public static String writeDeclareFieldDiffOrder(String itemName, String requestID, Map<String,DiffAlgorithm[]> algorithmsMap) throws RemotingException {
        StringBuilder sb = new StringBuilder();

        sb.append(METHOD_DECLARE_FIELD_DIFF_ORDER);
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(encodeString(itemName));
        sb.append(SEP);
        sb.append(TYPE_STRING);
        sb.append(SEP);
        sb.append(requestID);

        for (String name : algorithmsMap.keySet()) {
            sb.append(SEP);
            sb.append(TYPE_STRING);
            sb.append(SEP);
            sb.append(encodeString(name));

            DiffAlgorithm[] algs = algorithmsMap.get(name);
            sb.append(SEP);
            sb.append(TYPE_DIFF_ALGORITHMS);
            sb.append(SEP);
            sb.append(encodeAlgorithms(algs));
        }

        return sb.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // Internal methods

    protected static String encodeAlgorithms(DiffAlgorithm[] algs) throws RemotingException {
        if (algs == null) {
            return VALUE_NULL;
        }
        if (algs.length == 0) {
            return VALUE_EMPTY;
        }

        StringBuilder encodedAlgs = new StringBuilder();

        for (int i = 0; i < algs.length; i++) {
            switch (algs[i]) {
                case JSONPATCH:
                    encodedAlgs.append(TYPE_ALGORITHM_JSONPATCH);
                    break;
                case DIFF_MATCH_PATCH:
                    encodedAlgs.append(TYPE_ALGORITHM_DIFFMATCHPATCH);
                    break;
                default:
                    // null?
                    throw new RemotingException("Unknown diff algorithm '" + algs[i].toString() + "' found while encoding Diff Algorithms array");
            }
        }

        return encodedAlgs.toString();
    }

}