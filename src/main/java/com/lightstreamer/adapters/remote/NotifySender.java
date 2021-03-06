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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.lightstreamer.log.LogManager;
import com.lightstreamer.log.Logger;

class NotifySender {
    private static final Logger _replog = LogManager.getLogger("com.lightstreamer.adapters.remote.RequestReply.replies");
    private static final Logger _notlog = LogManager.getLogger("com.lightstreamer.adapters.remote.RequestReply.notifications");
    private static final Logger _repKlog = LogManager.getLogger("com.lightstreamer.adapters.remote.RequestReply.replies.keepalives");
    private static final Logger _notKlog = LogManager.getLogger("com.lightstreamer.adapters.remote.RequestReply.notifications.keepalives");

    private static final String END_LINE = "\r\n";
    private static final String STOP_WAITING_PILL = "STOP_WAITING_PILL";
    private static final String KEEPALIVE_PILL = "KEEPALIVE_PILL";
    
    private String _name;

    private BlockingDeque<String> _queue = new LinkedBlockingDeque<String>();
    private OutputStreamWriter _writer;
    private final boolean _repliesNotNotifies;
    private volatile int _keepaliveMillis;

    private ExceptionListener _exceptionListener;

    private boolean _stop;

    public NotifySender(String name, OutputStream notifyStream, int keepaliveMillis, ExceptionListener exceptionListener) {
        this(name, notifyStream, false, keepaliveMillis, exceptionListener);
    }

    public NotifySender(String name, OutputStream notifyStream, boolean repliesNotNotifies, int keepaliveMillis, ExceptionListener exceptionListener) {
        _name = name;

        try {
            _writer = new OutputStreamWriter(notifyStream, "UTF-8");
        } catch (UnsupportedEncodingException e) {
        }
        _repliesNotNotifies = repliesNotNotifies;
        _keepaliveMillis = keepaliveMillis;

        _exceptionListener = exceptionListener;
        
        _stop = false;
    }
    
    public void changeKeepalive(int keepaliveMillis, boolean alsoInterrupt) {
        _keepaliveMillis = keepaliveMillis;
        if (alsoInterrupt) {
            try {
                // interrupts the current wait as though a keepalive were needed;
                // in most cases, this keepalive will be redundant
                _queue.putFirst(KEEPALIVE_PILL);
            } catch (InterruptedException e) {
            }
        }
    }

    private Logger getProperLogger() {
        return _repliesNotNotifies ? _replog : _notlog;
    }

    private Logger getProperKeepaliveLogger() {
        return _repliesNotNotifies ? _repKlog : _notKlog;
    }

    private String getProperType() {
        return _repliesNotNotifies ? "Reply" : "Notify";
    }

    public final void start() {
        Thread t = new Thread() {
            public void run() {
                doRun();
            }
        };
        t.start();
    }

    public final void doRun() {
        getProperLogger().info(getProperType() + " sender '" + _name + "' starting...");

        while (!_stop) { //might as well be while(true)
            
            String reply;
            try {
                /*reply = _queue.pollFirst();
                if(reply == null) { //currently empty
                    _writer.flush();*/
                if (_keepaliveMillis > 0) {
                    reply = _queue.pollFirst(_keepaliveMillis, TimeUnit.MILLISECONDS);
                } else {
                    reply = _queue.takeFirst();
                }
                //}
                
            } catch (InterruptedException e) {
                _exceptionListener.onException(new RemotingException("Exception caught while waiting on the " + getProperType().toLowerCase() + " queue: " + e.getMessage(), e));
                break;
            }
            
            if (reply == STOP_WAITING_PILL) {
                break;
            }
 
            if (reply == null || reply == KEEPALIVE_PILL) {
                // the timeout (real or simulated) has fired
                reply = RemotingProtocol.METHOD_KEEPALIVE;
                if (getProperKeepaliveLogger().isDebugEnabled()) {
                    getProperKeepaliveLogger().debug(getProperType() + " line: " + reply);
                }
            } else {
                if (getProperLogger().isDebugEnabled()) {
                    getProperLogger().debug(getProperType() + " line: " + reply);
                }
            }
            
            try {
                _writer.write(reply);
                _writer.write(END_LINE);
                _writer.flush(); //use the above commented code to reduce flush calls
                
            } catch (IOException e) {
                _exceptionListener.onException(new RemotingException("Exception caught while writing on the " + getProperType().toLowerCase() + " stream: " + e.getMessage(), e));
                break;
            }
        }
        
        getProperLogger().info(getProperType() + " sender '" + _name + "' stopped");
        
    }

    public final void quit() {
        _stop = true;
        try {
            //set the stop pill as first thing
            _queue.putFirst(STOP_WAITING_PILL);
        } catch (InterruptedException e) {
        }
    }

    public final void sendNotify(String notify) {
        if (!_repliesNotNotifies) {
            long millis = new Date().getTime(); 

            StringBuilder timedNotify = new StringBuilder();
            timedNotify.append(millis);
            timedNotify.append(RemotingProtocol.SEP);
            timedNotify.append(notify);

            notify = timedNotify.toString();
        }

        //enqueue
        try {
            _queue.putLast(notify);
        } catch (InterruptedException e) {
        }

    }
}
