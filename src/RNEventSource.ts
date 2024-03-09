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

'use strict';

// @ts-ignore
// no typings for Networking module
import { Networking } from 'react-native';

import EventTarget from 'event-target-shim';

import {
    DidCompleteNetworkResponse,
    DidReceiveNetworkIncrementalData,
    DidReceiveNetworkResponse,
    EventSourceInitDict,
    EventSourceState,
    ExtendedEventSource,
    Headers,
    NetworkingListeners,
    EventSourceEvent,
} from './types';

const EVENT_SOURCE_EVENTS = [
    EventSourceEvent.ERROR,
    EventSourceEvent.MESSAGE,
    EventSourceEvent.OPEN,
    EventSourceEvent.STATE
];

// char codes
const bom: number[] = [239, 187, 191]; // byte order mark
const lf: number = 10;
const cr: number = 13;

const maxRetryAttempts: number = 5;
/**
 * An RCTNetworking-based implementation of the EventSource web standard.
 *
 * See https://developer.mozilla.org/en-US/docs/Web/API/EventSource
 *     https://html.spec.whatwg.org/multipage/server-sent-events.html
 *     https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
 */

class RNEventSource extends (EventTarget(EVENT_SOURCE_EVENTS)) implements ExtendedEventSource {
    static CONNECTING: number = EventSourceState.CONNECTING;
    static OPEN: number = EventSourceState.OPEN;
    static CLOSED: number = EventSourceState.CLOSED;

    CONNECTING: number = EventSourceState.CONNECTING;
    OPEN: number = EventSourceState.OPEN;
    CLOSED: number = EventSourceState.CLOSED;

    // Properties
    readyState: number = EventSourceState.CONNECTING;
    url: string;
    withCredentials: boolean = false;

    // Event handlers
    onerror = null;
    onmessage = null;
    onopen = null;

    // Buffers for event stream parsing
    private _isFirstChunk = false;
    private _discardNextLineFeed = false;
    private _lineBuf: string = '';
    private _dataBuf: string = '';
    private _eventTypeBuf: string = '';
    private _lastEventIdBuf: string = '';

    private _headers: Headers = {};
    private _lastEventId: string = '';
    private _reconnectIntervalMs: number = 1000;
    private _requestId?: number | null;
    private _subscriptions: any[];
    private _trackingName: string = 'unknown';
    private _retryAttempts: number = 0;

    setTrackingName(trackingName: string) {
        this._trackingName = trackingName;

        return this;
    }

    constructor(url: string, eventSourceInitDict?: EventSourceInitDict) {
        super();

        if (!url) {
            throw new Error('Cannot open an SSE stream on an empty url');
        }
        this.url = url;

        this._headers['Cache-Control'] = 'no-store';
        this._headers.Accept = 'text/event-stream';

        if (this._lastEventId) {
            this._headers = this.__mergeHeaders({ 'Last-Event-ID': this._lastEventId });
        }

        if (eventSourceInitDict) {
            if (eventSourceInitDict.headers) {
                if (eventSourceInitDict.headers['Last-Event-ID']) {
                    this._lastEventId = eventSourceInitDict.headers['Last-Event-ID'];
                    delete eventSourceInitDict.headers['Last-Event-ID'];
                }

                for (var headerKey in eventSourceInitDict.headers) {
                    const header = eventSourceInitDict.headers[headerKey];
                    if (header) {
                        this._headers[headerKey] = header;
                    }
                }
            }

            if (eventSourceInitDict.withCredentials) {
                this.withCredentials = eventSourceInitDict.withCredentials;
            }
        }

        this._subscriptions = [];
        this.__createSubscriptions();

        this.__connect();
    }

    close(): void {
        if (this._requestId !== null && this._requestId !== undefined) {
            Networking.abortRequest(this._requestId);
        }

        // clean up Networking subscriptions
        (this._subscriptions || []).forEach(sub => {
            if (sub) {
                sub.remove();
            }
        });
        this._subscriptions = [];

        this.__changeReadyState(EventSourceState.CLOSED);
    }

    connect(): void {
        if (this._subscriptions.length === 0) {
            this.__createSubscriptions();
        }

        this.__connect();
    }

    reconnect(reason?: string): void {
        if (this._subscriptions.length === 0) {
            this.__createSubscriptions();
        }

        this.__reconnect(reason);
    }

    changeReadyState(state: number) {
        this.__changeReadyState(state);
    }

    private __createSubscriptions() {
        this._subscriptions.push(
            Networking.addListener(
                NetworkingListeners.DidReceiveNetworkResponse,
                (args: DidReceiveNetworkResponse) => this.__didReceiveResponse(...args),
            ),
        );
        this._subscriptions.push(
            Networking.addListener(
                NetworkingListeners.DidReceiveNetworkIncrementalData,
                (args: DidReceiveNetworkIncrementalData) => this.__didReceiveIncrementalData(...args),
            ),
        );
        this._subscriptions.push(
            Networking.addListener(
                NetworkingListeners.DidCompleteNetworkResponse,
                (args: DidCompleteNetworkResponse) => this.__didCompleteResponse(...args),
            ),
        );
    }

    private __connect(): void | null {
        if (this.readyState === EventSourceState.CLOSED) {
            return null;
        }

        if (this._lastEventId) {
            this._headers = this.__mergeHeaders({ 'Last-Event-ID': this._lastEventId });
        }

        Networking.sendRequest(
            'GET', // EventSource always GETs the resource
            this._trackingName,
            this.url,
            this._headers,
            '', // body for EventSource request is always empty
            'text', // SSE is a text protocol
            true, // we want incremental events
            0, // there is no timeout defined in the WHATWG spec for EventSource
            this.__didCreateRequest.bind(this),
            this.withCredentials,
        );
    }

    private __reconnect(reason?: string): void {
        this.__changeReadyState(EventSourceState.CONNECTING);

        const errorEventMessage = `reestablishing connection${reason ? ': ' + reason : ''}`;

        this.dispatchEvent({ type: EventSourceEvent.ERROR, data: errorEventMessage });

        if (this._reconnectIntervalMs > 0) {
            setTimeout(this.__connect.bind(this), this._reconnectIntervalMs);
        } else {
            this.__connect();
        }
    }

    private __changeReadyState(readyState: EventSourceState) {
        this.readyState = readyState;

        this.dispatchEvent({
            type: EventSourceEvent.STATE,
            data: readyState,
        });
    }

    private __mergeHeaders(headers: EventSourceInitDict['headers']) {
        return Object.assign({}, this._headers, headers);
    }

    // Internal buffer processing methods

    private __processEventStreamChunk(chunk: string): void {
        if (this._isFirstChunk) {
            if (
                bom.every((charCode, idx) => {
                    return this._lineBuf.charCodeAt(idx) === charCode;
                })
            ) {
                // Strip byte order mark from chunk
                chunk = chunk.slice(bom.length);
            }
            this._isFirstChunk = false;
        }

        let pos: number = 0;
        while (pos < chunk.length) {
            if (this._discardNextLineFeed) {
                if (chunk.charCodeAt(pos) === lf) {
                    // Ignore this LF since it was preceded by a CR
                    ++pos;
                }
                this._discardNextLineFeed = false;
            }

            const curCharCode = chunk.charCodeAt(pos);

            if (curCharCode === cr || curCharCode === lf) {
                this.__processEventStreamLine();

                // Treat CRLF properly
                if (curCharCode === cr) {
                    this._discardNextLineFeed = true;
                }
            } else {
                this._lineBuf += chunk.charAt(pos);
            }

            ++pos;
        }
    }

    private __processEventStreamLine(): void {
        const line = this._lineBuf;

        // clear the line buffer
        this._lineBuf = '';

        // Dispatch the buffered event if this is an empty line
        if (line === '') {
            this.__dispatchBufferedEvent();
            return;
        }

        const colonPos = line.indexOf(':');

        let field: string;
        let value: string;

        if (colonPos === 0) {
            // this is a comment line and should be ignored
            return;
        } else if (colonPos > 0) {
            if (line[colonPos + 1] === ' ') {
                field = line.slice(0, colonPos);
                value = line.slice(colonPos + 2); // ignores the first space from the value
            } else {
                field = line.slice(0, colonPos);
                value = line.slice(colonPos + 1);
            }
        } else {
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
                const newRetryMs = parseInt(value, 10);
                if (!isNaN(newRetryMs)) {
                    this._reconnectIntervalMs = newRetryMs;
                }
                break;
            default:
            // this is an unrecognized field, so this line should be ignored
        }
    }

    private __dispatchBufferedEvent() {
        this._lastEventId = this._lastEventIdBuf;

        // Dispatch the event
        const eventType = this._eventTypeBuf || EventSourceEvent.MESSAGE;
        this.dispatchEvent({
            type: eventType,
            data: this._dataBuf.slice(0, -1), // remove the trailing LF from the data
            origin: this.url,
            lastEventId: this._lastEventId,
        });

        // Reset the data and event type buffers
        this._dataBuf = '';
        this._eventTypeBuf = '';
    }

    // Networking callbacks, exposed for testing

    private __didCreateRequest(requestId: number): void {
        this._requestId = requestId;
    }

    private __didReceiveResponse(
        requestId: number,
        status: number,
        responseHeaders?: Headers,
        responseURL?: string,
    ): boolean {
        if (requestId !== this._requestId) {
            return false;
        }

        if (responseHeaders) {
            // make the header names case insensitive
            for (const entry of Object.entries(responseHeaders)) {
                const [key, value] = entry;
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
            } else {
                this.close();
                return this.dispatchEvent({
                    type: EventSourceEvent.ERROR,
                    data: 'got redirect with no location',
                });
            }
        }

        if (status !== 200) {
            this.close();

            return this.dispatchEvent({
                type: EventSourceEvent.ERROR,
                data: 'unexpected HTTP status ' + status,
            });
        }

        if (
            responseHeaders &&
            responseHeaders['content-type'] !== 'text/event-stream'
        ) {
            this.close();
            return this.dispatchEvent({
                type: EventSourceEvent.ERROR,
                data:
                    'unsupported MIME type in response: ' +
                    responseHeaders['content-type'],
            });
        } else if (!responseHeaders) {
            this.close();

            return this.dispatchEvent({
                type: EventSourceEvent.ERROR,
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

        this.__changeReadyState(EventSourceState.OPEN);
        return this.dispatchEvent({ type: EventSourceEvent.OPEN });
    }

    private __didReceiveIncrementalData(
        requestId: number,
        responseText: string,
        progress: number,
        total: number,
    ) {
        if (requestId !== this._requestId) {
            return;
        }

        this.__processEventStreamChunk(responseText);
    }

    private __didCompleteResponse(
        requestId: number,
        error: string,
        timeOutError: boolean,
    ): void {
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
        } else {
            this.close();
            this.dispatchEvent({
                type: EventSourceEvent.ERROR,
                data: 'could not reconnect after ' + maxRetryAttempts + ' attempts',
            });
        }
    }
}

export default RNEventSource;
