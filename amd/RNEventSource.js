/**
 * Copyright (c) 2020 Adam Chelminski and Nepein Andrey
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
define(["require", "exports", "react-native", "event-target-shim", "./types"], function (require, exports, react_native_1, event_target_shim_1, types_1) {
    'use strict';
    Object.defineProperty(exports, "__esModule", { value: true });
    event_target_shim_1 = __importDefault(event_target_shim_1);
    var EVENT_SOURCE_EVENTS = [
        types_1.EventSourceEvent.ERROR,
        types_1.EventSourceEvent.MESSAGE,
        types_1.EventSourceEvent.OPEN,
        types_1.EventSourceEvent.STATE
    ];
    // char codes
    var bom = [239, 187, 191]; // byte order mark
    var lf = 10;
    var cr = 13;
    var maxRetryAttempts = 5;
    /**
     * An RCTNetworking-based implementation of the EventSource web standard.
     *
     * See https://developer.mozilla.org/en-US/docs/Web/API/EventSource
     *     https://html.spec.whatwg.org/multipage/server-sent-events.html
     *     https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
     */
    var RNEventSource = /** @class */ (function (_super) {
        __extends(RNEventSource, _super);
        function RNEventSource(url, eventSourceInitDict) {
            var _this = _super.call(this) || this;
            _this.CONNECTING = types_1.EventSourceState.CONNECTING;
            _this.OPEN = types_1.EventSourceState.OPEN;
            _this.CLOSED = types_1.EventSourceState.CLOSED;
            // Properties
            _this.readyState = types_1.EventSourceState.CONNECTING;
            _this.withCredentials = false;
            // Event handlers
            _this.onerror = null;
            _this.onmessage = null;
            _this.onopen = null;
            // Buffers for event stream parsing
            _this._isFirstChunk = false;
            _this._discardNextLineFeed = false;
            _this._lineBuf = '';
            _this._dataBuf = '';
            _this._eventTypeBuf = '';
            _this._lastEventIdBuf = '';
            _this._headers = {};
            _this._lastEventId = '';
            _this._reconnectIntervalMs = 1000;
            _this._trackingName = 'unknown';
            _this._retryAttempts = 0;
            if (!url) {
                throw new Error('Cannot open an SSE stream on an empty url');
            }
            _this.url = url;
            _this._headers['Cache-Control'] = 'no-store';
            _this._headers.Accept = 'text/event-stream';
            if (_this._lastEventId) {
                _this._headers = _this.__mergeHeaders({ 'Last-Event-ID': _this._lastEventId });
            }
            if (eventSourceInitDict) {
                if (eventSourceInitDict.headers) {
                    if (eventSourceInitDict.headers['Last-Event-ID']) {
                        _this._lastEventId = eventSourceInitDict.headers['Last-Event-ID'];
                        delete eventSourceInitDict.headers['Last-Event-ID'];
                    }
                    for (var headerKey in eventSourceInitDict.headers) {
                        var header = eventSourceInitDict.headers[headerKey];
                        if (header) {
                            _this._headers[headerKey] = header;
                        }
                    }
                }
                if (eventSourceInitDict.withCredentials) {
                    _this.withCredentials = eventSourceInitDict.withCredentials;
                }
            }
            _this._subscriptions = [];
            _this.__createSubscriptions();
            _this.__connect();
            return _this;
        }
        RNEventSource.prototype.setTrackingName = function (trackingName) {
            this._trackingName = trackingName;
            return this;
        };
        RNEventSource.prototype.close = function () {
            if (this._requestId !== null && this._requestId !== undefined) {
                react_native_1.Networking.abortRequest(this._requestId);
            }
            // clean up Networking subscriptions
            (this._subscriptions || []).forEach(function (sub) {
                if (sub) {
                    sub.remove();
                }
            });
            this._subscriptions = [];
            this.__changeReadyState(types_1.EventSourceState.CLOSED);
        };
        RNEventSource.prototype.connect = function () {
            if (this._subscriptions.length === 0) {
                this.__createSubscriptions();
            }
            this.__connect();
        };
        RNEventSource.prototype.reconnect = function (reason) {
            if (this._subscriptions.length === 0) {
                this.__createSubscriptions();
            }
            this.__reconnect(reason);
        };
        RNEventSource.prototype.changeReadyState = function (state) {
            this.__changeReadyState(state);
        };
        RNEventSource.prototype.__createSubscriptions = function () {
            var _this = this;
            this._subscriptions.push(react_native_1.Networking.addListener(types_1.NetworkingListeners.DidReceiveNetworkResponse, function (args) { return _this.__didReceiveResponse.apply(_this, args); }));
            this._subscriptions.push(react_native_1.Networking.addListener(types_1.NetworkingListeners.DidReceiveNetworkIncrementalData, function (args) { return _this.__didReceiveIncrementalData.apply(_this, args); }));
            this._subscriptions.push(react_native_1.Networking.addListener(types_1.NetworkingListeners.DidCompleteNetworkResponse, function (args) { return _this.__didCompleteResponse.apply(_this, args); }));
        };
        RNEventSource.prototype.__connect = function () {
            if (this.readyState === types_1.EventSourceState.CLOSED) {
                return null;
            }
            if (this._lastEventId) {
                this._headers = this.__mergeHeaders({ 'Last-Event-ID': this._lastEventId });
            }
            react_native_1.Networking.sendRequest('GET', // EventSource always GETs the resource
            this._trackingName, this.url, this._headers, '', // body for EventSource request is always empty
            'text', // SSE is a text protocol
            true, // we want incremental events
            0, // there is no timeout defined in the WHATWG spec for EventSource
            this.__didCreateRequest.bind(this), this.withCredentials);
        };
        RNEventSource.prototype.__reconnect = function (reason) {
            this.__changeReadyState(types_1.EventSourceState.CONNECTING);
            var errorEventMessage = "reestablishing connection" + (reason ? ': ' + reason : '');
            this.dispatchEvent({ type: types_1.EventSourceEvent.ERROR, data: errorEventMessage });
            if (this._reconnectIntervalMs > 0) {
                setTimeout(this.__connect.bind(this), this._reconnectIntervalMs);
            }
            else {
                this.__connect();
            }
        };
        RNEventSource.prototype.__changeReadyState = function (readyState) {
            this.readyState = readyState;
            this.dispatchEvent({
                type: types_1.EventSourceEvent.STATE,
                data: readyState,
            });
        };
        RNEventSource.prototype.__mergeHeaders = function (headers) {
            return Object.assign({}, this._headers, headers);
        };
        // Internal buffer processing methods
        RNEventSource.prototype.__processEventStreamChunk = function (chunk) {
            var _this = this;
            if (this._isFirstChunk) {
                if (bom.every(function (charCode, idx) {
                    return _this._lineBuf.charCodeAt(idx) === charCode;
                })) {
                    // Strip byte order mark from chunk
                    chunk = chunk.slice(bom.length);
                }
                this._isFirstChunk = false;
            }
            var pos = 0;
            while (pos < chunk.length) {
                if (this._discardNextLineFeed) {
                    if (chunk.charCodeAt(pos) === lf) {
                        // Ignore this LF since it was preceded by a CR
                        ++pos;
                    }
                    this._discardNextLineFeed = false;
                }
                var curCharCode = chunk.charCodeAt(pos);
                if (curCharCode === cr || curCharCode === lf) {
                    this.__processEventStreamLine();
                    // Treat CRLF properly
                    if (curCharCode === cr) {
                        this._discardNextLineFeed = true;
                    }
                }
                else {
                    this._lineBuf += chunk.charAt(pos);
                }
                ++pos;
            }
        };
        RNEventSource.prototype.__processEventStreamLine = function () {
            var line = this._lineBuf;
            // clear the line buffer
            this._lineBuf = '';
            // Dispatch the buffered event if this is an empty line
            if (line === '') {
                this.__dispatchBufferedEvent();
                return;
            }
            var colonPos = line.indexOf(':');
            var field;
            var value;
            if (colonPos === 0) {
                // this is a comment line and should be ignored
                return;
            }
            else if (colonPos > 0) {
                if (line[colonPos + 1] === ' ') {
                    field = line.slice(0, colonPos);
                    value = line.slice(colonPos + 2); // ignores the first space from the value
                }
                else {
                    field = line.slice(0, colonPos);
                    value = line.slice(colonPos + 1);
                }
            }
            else {
                field = line;
                value = '';
            }
            switch (field) {
                case 'event':
                    // Set the type of this event
                    this._eventTypeBuf = value;
                    break;
                case 'data':
                    // Append the line to the data buffer along with an LF (U+000A)
                    this._dataBuf += value;
                    this._dataBuf += String.fromCodePoint(lf);
                    break;
                case 'id':
                    // Update the last seen event id
                    this._lastEventIdBuf = value;
                    break;
                case 'retry':
                    // Set a new reconnect interval value
                    var newRetryMs = parseInt(value, 10);
                    if (!isNaN(newRetryMs)) {
                        this._reconnectIntervalMs = newRetryMs;
                    }
                    break;
                default:
                // this is an unrecognized field, so this line should be ignored
            }
        };
        RNEventSource.prototype.__dispatchBufferedEvent = function () {
            this._lastEventId = this._lastEventIdBuf;
            // Dispatch the event
            var eventType = this._eventTypeBuf || types_1.EventSourceEvent.MESSAGE;
            this.dispatchEvent({
                type: eventType,
                data: this._dataBuf.slice(0, -1),
                origin: this.url,
                lastEventId: this._lastEventId,
            });
            // Reset the data and event type buffers
            this._dataBuf = '';
            this._eventTypeBuf = '';
        };
        // Networking callbacks, exposed for testing
        RNEventSource.prototype.__didCreateRequest = function (requestId) {
            this._requestId = requestId;
        };
        RNEventSource.prototype.__didReceiveResponse = function (requestId, status, responseHeaders, responseURL) {
            if (requestId !== this._requestId) {
                return false;
            }
            if (responseHeaders) {
                // make the header names case insensitive
                for (var _i = 0, _a = Object.entries(responseHeaders); _i < _a.length; _i++) {
                    var entry = _a[_i];
                    var key = entry[0], value = entry[1];
                    delete responseHeaders[key];
                    responseHeaders[key.toLowerCase()] = value;
                }
            }
            // Handle redirects
            if (status === 301 || status === 307) {
                if (responseHeaders && responseHeaders.location) {
                    // set the new URL, set the requestId to null so that request
                    // completion doesn't attempt a reconnect, and immediately attempt
                    // reconnecting
                    this.url = responseHeaders.location;
                    this._requestId = null;
                    this.__connect();
                    return false;
                }
                else {
                    this.close();
                    return this.dispatchEvent({
                        type: types_1.EventSourceEvent.ERROR,
                        data: 'got redirect with no location',
                    });
                }
            }
            if (status !== 200) {
                this.close();
                return this.dispatchEvent({
                    type: types_1.EventSourceEvent.ERROR,
                    data: 'unexpected HTTP status ' + status,
                });
            }
            if (responseHeaders &&
                responseHeaders['content-type'] !== 'text/event-stream') {
                this.close();
                return this.dispatchEvent({
                    type: types_1.EventSourceEvent.ERROR,
                    data: 'unsupported MIME type in response: ' +
                        responseHeaders['content-type'],
                });
            }
            else if (!responseHeaders) {
                this.close();
                return this.dispatchEvent({
                    type: types_1.EventSourceEvent.ERROR,
                    data: 'no MIME type in response',
                });
            }
            // reset the connection retry attempt counter
            this._retryAttempts = 0;
            // reset the stream processing buffers
            this._isFirstChunk = false;
            this._discardNextLineFeed = false;
            this._lineBuf = '';
            this._dataBuf = '';
            this._eventTypeBuf = '';
            this._lastEventIdBuf = '';
            this.__changeReadyState(types_1.EventSourceState.OPEN);
            return this.dispatchEvent({ type: types_1.EventSourceEvent.OPEN });
        };
        RNEventSource.prototype.__didReceiveIncrementalData = function (requestId, responseText, progress, total) {
            if (requestId !== this._requestId) {
                return;
            }
            this.__processEventStreamChunk(responseText);
        };
        RNEventSource.prototype.__didCompleteResponse = function (requestId, error, timeOutError) {
            if (requestId !== this._requestId) {
                return;
            }
            // The spec states: 'Network errors that prevents the connection from being
            // established in the first place (e.g. DNS errors), should cause the user
            // agent to reestablish the connection in parallel, unless the user agent
            // knows that to be futile, in which case the user agent may fail the
            // connection.'
            //
            // We are treating 5 unnsuccessful retry attempts as a sign that attempting
            // to reconnect is 'futile'. Future improvements could also add exponential
            // backoff.
            if (this._retryAttempts < maxRetryAttempts) {
                // pass along the error message so that the user sees it as part of the
                // error event fired for re-establishing the connection
                this._retryAttempts += 1;
                this.__reconnect(error);
            }
            else {
                this.close();
                this.dispatchEvent({
                    type: types_1.EventSourceEvent.ERROR,
                    data: 'could not reconnect after ' + maxRetryAttempts + ' attempts',
                });
            }
        };
        RNEventSource.CONNECTING = types_1.EventSourceState.CONNECTING;
        RNEventSource.OPEN = types_1.EventSourceState.OPEN;
        RNEventSource.CLOSED = types_1.EventSourceState.CLOSED;
        return RNEventSource;
    }((event_target_shim_1.default(EVENT_SOURCE_EVENTS))));
    exports.default = RNEventSource;
});
