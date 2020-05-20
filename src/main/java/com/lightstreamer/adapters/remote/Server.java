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

import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.lightstreamer.log.LogManager;
import com.lightstreamer.log.LoggerProvider;


/** 
 * A generic Remote Server object, which can run a Remote Data or Metadata Adapter
 * and connect it to the Proxy Adapter running on Lightstreamer Server. <BR>
 * The object should be provided with a suitable Adapter instance
 * and with suitable initialization parameters and established connections,
 * then activated through {@link #start} and finally disposed through {@link #close}.
 * Further reuse of the same instance is not supported.<BR>
 * The Remote Server will take care of sending keepalive packets on the connections
 * when needed. The interval can be configured through the custom
 * "lightstreamer.keepalive.millis" system property, where a value of 0
 * or negative means no keepalives. By default, it is set to 10000 ms. <BR>
 * However, if a stricter interval is requested by the Proxy Adapter on startup,
 * it will be obeyed (with a safety minimum of 1 second). This should ensure
 * that the Proxy Adapter activity checks will always succeed, but for some
 * old versions of the Proxy Adapter. 
 */
public abstract class Server {

    private ServerImpl _impl;
    private boolean startedOnce = false;

    final void init(ServerImpl impl) {
        _impl = impl;
    }

    /** 
     * Sets a name for the Server instance; used for logging purposes. 
     * 
     * @param name a name to identify the instance.
     */
    public final void setName(@Nonnull String name) {
        if (startedOnce) {
            throw new IllegalStateException("Reuse of Server object forbidden");
        }
        _impl.setName(name);
    }
    
    /** 
     * Gets the name, used for logging purposes, associated to the Server instance. 
     * 
     * @return the name for the Server instance.
     */
    @Nonnull
    public final String getName() {
        return _impl.getName();
    }

    /** 
     * Sets the user-name credential to be sent to the Proxy Adapter upon connection.
     * The credentials are needed only if the Proxy Adapter is configured
     * to require Remote Adapter authentication.
     * The credentials will be sent only if both are non-null.<BR>
     * 
     * The default value is null.
     * 
     * @param user a user name.
     */
    public final void setRemoteUser(@Nullable String user) {
        _impl.setRemoteUser(user);
    }
    /** 
     * Sets the password credential to be sent to the Proxy Adapter upon connection.
     * The credentials are needed only if the Proxy Adapter is configured
     * to require Remote Adapter authentication.
     * The credentials will be sent only if both are non-null.<BR>
     *
     * The default value is null.
     * 
     * @param password a password.
     */
    public final void setRemotePassword(@Nullable String password) {
        _impl.setRemotePassword(password);
    }
    /** 
     * Gets the user-name credential to be sent to the Proxy Adapter upon connection.
     * The credentials will be sent only if both are non-null.<BR>
     * 
     * The default value is null.
     * 
     * @return the user-name credential or null.
     */
    @Nullable
    public final String getRemoteUser() {
        return _impl.getRemoteUser();
    }
    /** 
     * Gets the password credential to be sent to the Proxy Adapter upon connection.
     * The credentials will be sent only if both are non-null.<BR>
     * 
     * The default value is null.
     * 
     * @return the password credential or null.
     */
    @Nullable
    public final String getRemotePassword() {
        return _impl.getRemotePassword();
    }

    /** 
     * Sets the stream used by the Proxy Adapter in order to forward the requests
     * to the Remote Adapter. 
     * 
     * @param inputStream the stream used by the Proxy Adapter in order to forward the requests
     * to the Remote Adapter
     */
    public final void setRequestStream(@Nonnull InputStream inputStream) {
        if (startedOnce) {
            throw new IllegalStateException("Reuse of Server object forbidden");
        }
        _impl.setRequestStream(inputStream);
    }
    /** 
     * Gets the stream used by the Proxy Adapter in order to forward the requests
     * to the Remote Adapter. 
     * 
     * @return the stream used by the Proxy Adapter in order to forward the requests
     * to the Remote Adapter
     */
    @Nonnull
    public final InputStream getRequestStream() {
        return _impl.getRequestStream();
    }

    /** 
     * Sets the stream used by the Remote Adapter in order to forward the answers
     * to the Proxy Adapter. 
     * 
     * @param outputStream the stream used by the Remote Adapter in order to forward the answers
     * to the Proxy Adapter. 
     */
    public final void setReplyStream(@Nonnull OutputStream outputStream) {
        if (startedOnce) {
            throw new IllegalStateException("Reuse of Server object forbidden");
        }
        _impl.setReplyStream(outputStream);
    }
    /** 
     * Gets the stream used by the Remote Adapter in order to forward the answers
     * to the Proxy Adapter. 
     * 
     * @return the stream used by the Remote Adapter in order to forward the answers
     * to the Proxy Adapter. 
     */
    @Nonnull
    public final OutputStream getReplyStream() {
        return _impl.getReplyStream();
    }

    /** 
     * Sets the stream used by the Remote Adapter in order to send asyncronous
     * data to the Remote Adapter. Currently not used and not needed
     * by the Remote Metadata Adapter.
     * 
     * @param outputStream the stream used by the Remote Adapter in order to send asyncronous
     * data to the Remote Adapter. 
     */
    public final void setNotifyStream(@Nonnull OutputStream outputStream) {
        if (startedOnce) {
            throw new IllegalStateException("Reuse of Server object forbidden");
        }
        _impl.setNotifyStream(outputStream);
    }
    /** 
     * Gets the stream used by the Remote Adapter in order to send asyncronous
     * data to the Remote Adapter. Currently not used and not needed
     * by the Remote Metadata Adapter.
     * 
     * @return the stream used by the Remote Adapter in order to send asyncronous
     * data to the Remote Adapter. 
     */
    @Nonnull
    public final OutputStream getNotifyStream() {
        return _impl.getNotifyStream();
    }

    /** 
     * Sets the handler for error conditions occurring on the Remote Server.
     * By setting the handler, it's possible to override the default
     * exception handling.
     * 
     * @param handler the handler for error conditions occurring on the Remote Server.
     */
    public final void setExceptionHandler(@Nullable ExceptionHandler handler) {
        _impl.setExceptionHandler(handler);
    }
    /** 
     * Gets the handler for error conditions occurring on the Remote Server.
     * 
     * @return the handler for error conditions occurring on the Remote Server.
     */
    @Nullable
    public final ExceptionHandler getExceptionHandler() {
        return _impl.getExceptionHandler();
    }

    /** 
     * Starts the communication between the Remote Adapter and the Proxy Adapter
     * through the supplied streams. If requested by the initializeOnStart flag
     * in the instance constructor, the Remote Adapter is initialized immediately.
     * Then, requests issued by the Proxy Adapter are received and forwarded
     * to the Remote Adapter. If the Remote Adapter is not initialized
     * immediately, initialization will be triggered by the Proxy Adapter
     * and any initialization error will be just notified to the Proxy Adapter.
     * @exception RemotingException An error occurred in the initialization
     * phase. The adapter was not started.
     * @exception DataProviderException An error occurred in the initialization
     * phase. The adapter was not started. Only possible when the Adapter is
     * initialized immediately. 
     * @exception MetadataProviderException An error occurred in the initialization
     * phase. The adapter was not started. Only possible when the Adapter is
     * initialized immediately.
     */
    public final void start() throws RemotingException, DataProviderException, MetadataProviderException {
        if (startedOnce) {
            throw new IllegalStateException("Reuse of Server object forbidden");
        }
        startedOnce = true;
        try {
            _impl.start();
        } catch (RemotingException | DataProviderException | MetadataProviderException e) {
            _impl.stop();
            throw e;
        }
    }

    /** 
     * Stops the management of the Remote Adapter and destroys
     * the threads used by this Server. This instance can no longer
     * be used. <BR>
     * The streams supplied to this instance are also closed. <BR>
     * Note that this does not stop the supplied Remote Adapter,
     * as no close method is available in the Remote Adapter interface.
     * If the process is not terminating, then the Remote Adapter
     * cleanup should be performed by accessing the supplied Adapter
     * instance directly and calling custom methods. <BR>
    */
    public final void close() {
        _impl.stop();
        _impl.dispose();
        _impl = null;
    }

    /** 
     * Sets the LoggerProvider instance that will be used by the classes of the library to obtain Logger instances
     * used to propagate internal logging. Providing a new provider to the library permits to consume the log produced through
     * custom Logger implementation. <BR>
     * As soon as a new LoggerProvider is provided all the instances of Logger already in use in the
     * library are discarded and substituted with instanced obtained from this new instance. If a null value is provided,
     * the default consumers, that discard all the log, are enabled.
     * 
     * @param loggerProvider Will be responsible to provide Logger instances to the various classes of the library.
    */
    public static void setLoggerProvider(@Nullable LoggerProvider loggerProvider) {
        LogManager.setLoggerProvider(loggerProvider);
    }

}