/**
 * API Client Functions
 * Owner: First builder (shared HTTP client)
 * Co-owner: Scenario 17 - Network Error Handling
 *
 * Expected exports:
 * - fetchStatus() - GET /api/status
 * - fetchKeys(page, limit) - GET /api/keys
 * - fetchKey(key) - GET /api/keys/{key}
 * - setKey(key, value, flags, ttl) - POST /api/keys
 * - deleteKey(key) - DELETE /api/keys/{key}
 * - fetchConfig() - GET /api/config
 *
 * Network Error Handling (Scenario 17):
 * - Request timeout handling with configurable timeout
 * - Server unavailable detection
 * - Last known data caching
 * - Connectivity state tracking and recovery detection
 * - Event system for connectivity changes
 */

var MirDBApi = (function() {
    'use strict';

    var BASE_URL = '';
    var DEFAULT_TIMEOUT_MS = 10000; // 10 second default timeout

    // Network state tracking
    var networkState = {
        isOnline: true,
        lastError: null,
        lastSuccessTime: null,
        consecutiveFailures: 0
    };

    // Cache for last known data
    var dataCache = {
        status: null,
        keys: null,
        config: null
    };

    // Event listeners for connectivity changes
    var connectivityListeners = [];

    // Error types for network issues
    var ErrorTypes = {
        TIMEOUT: 'TIMEOUT',
        SERVER_UNAVAILABLE: 'SERVER_UNAVAILABLE',
        NETWORK_ERROR: 'NETWORK_ERROR',
        API_ERROR: 'API_ERROR'
    };

    /**
     * Custom error class for network errors
     * @param {string} message - Error message
     * @param {string} type - Error type from ErrorTypes
     * @param {Object} details - Additional error details
     */
    function NetworkError(message, type, details) {
        this.message = message;
        this.type = type;
        this.details = details || {};
        this.name = 'NetworkError';
        this.timestamp = new Date().toISOString();
    }
    NetworkError.prototype = Object.create(Error.prototype);
    NetworkError.prototype.constructor = NetworkError;

    /**
     * Fetch with timeout support
     * @param {string} url - URL to fetch
     * @param {Object} options - Fetch options
     * @param {number} timeout - Timeout in milliseconds
     * @returns {Promise<Response>} Fetch response
     */
    function fetchWithTimeout(url, options, timeout) {
        timeout = timeout || DEFAULT_TIMEOUT_MS;
        options = options || {};

        return new Promise(function(resolve, reject) {
            var controller = new AbortController();
            var timeoutId = setTimeout(function() {
                controller.abort();
                reject(new NetworkError(
                    'Connection timeout: Request took longer than ' + (timeout / 1000) + ' seconds',
                    ErrorTypes.TIMEOUT,
                    { url: url, timeout: timeout }
                ));
            }, timeout);

            options.signal = controller.signal;

            fetch(url, options)
                .then(function(response) {
                    clearTimeout(timeoutId);
                    resolve(response);
                })
                .catch(function(error) {
                    clearTimeout(timeoutId);
                    if (error.name === 'AbortError') {
                        reject(new NetworkError(
                            'Connection timeout: Request took longer than ' + (timeout / 1000) + ' seconds',
                            ErrorTypes.TIMEOUT,
                            { url: url, timeout: timeout }
                        ));
                    } else if (error.message && (
                        error.message.includes('Failed to fetch') ||
                        error.message.includes('NetworkError') ||
                        error.message.includes('Network request failed') ||
                        error.message.includes('net::ERR_')
                    )) {
                        reject(new NetworkError(
                            'Server unavailable: Unable to connect to the server',
                            ErrorTypes.SERVER_UNAVAILABLE,
                            { url: url, originalError: error.message }
                        ));
                    } else {
                        reject(new NetworkError(
                            'Network error: ' + error.message,
                            ErrorTypes.NETWORK_ERROR,
                            { url: url, originalError: error.message }
                        ));
                    }
                });
        });
    }

    /**
     * Update network state and notify listeners
     * @param {boolean} isOnline - Whether the network is online
     * @param {Object} error - Error object if offline
     */
    function updateNetworkState(isOnline, error) {
        var wasOnline = networkState.isOnline;
        networkState.isOnline = isOnline;

        if (isOnline) {
            networkState.lastError = null;
            networkState.consecutiveFailures = 0;
            networkState.lastSuccessTime = new Date().toISOString();
        } else {
            networkState.lastError = error;
            networkState.consecutiveFailures++;
        }

        // Notify listeners of connectivity change
        if (wasOnline !== isOnline) {
            notifyConnectivityChange(isOnline);
        }
    }

    /**
     * Notify all connectivity listeners
     * @param {boolean} isOnline - Whether the network is online
     */
    function notifyConnectivityChange(isOnline) {
        connectivityListeners.forEach(function(listener) {
            try {
                listener({
                    isOnline: isOnline,
                    timestamp: new Date().toISOString(),
                    lastError: networkState.lastError,
                    consecutiveFailures: networkState.consecutiveFailures
                });
            } catch (e) {
                console.error('Error in connectivity listener:', e);
            }
        });
    }

    /**
     * Add a connectivity change listener
     * @param {Function} listener - Callback function
     * @returns {Function} Unsubscribe function
     */
    function onConnectivityChange(listener) {
        if (typeof listener === 'function') {
            connectivityListeners.push(listener);
            return function() {
                var index = connectivityListeners.indexOf(listener);
                if (index > -1) {
                    connectivityListeners.splice(index, 1);
                }
            };
        }
        return function() {};
    }

    /**
     * Get current network state
     * @returns {Object} Network state object
     */
    function getNetworkState() {
        return {
            isOnline: networkState.isOnline,
            lastError: networkState.lastError,
            lastSuccessTime: networkState.lastSuccessTime,
            consecutiveFailures: networkState.consecutiveFailures
        };
    }

    /**
     * Get cached data for a specific endpoint
     * @param {string} endpoint - The endpoint name (status, keys, config)
     * @returns {Object|null} Cached data or null
     */
    function getCachedData(endpoint) {
        return dataCache[endpoint] || null;
    }

    /**
     * Update cache with new data
     * @param {string} endpoint - The endpoint name
     * @param {Object} data - Data to cache
     */
    function updateCache(endpoint, data) {
        dataCache[endpoint] = {
            data: data,
            timestamp: new Date().toISOString()
        };
    }

    /**
     * Fetch server status
     * @param {Object} options - Request options
     * @param {number} options.timeout - Request timeout in ms
     * @returns {Promise<Object>} Status response
     */
    function fetchStatus(options) {
        options = options || {};
        return fetchWithTimeout(BASE_URL + '/api/status', {}, options.timeout)
            .then(handleResponse)
            .then(function(data) {
                updateCache('status', data);
                updateNetworkState(true);
                return data;
            })
            .catch(function(error) {
                updateNetworkState(false, error);
                throw error;
            });
    }

    /**
     * Fetch paginated key list
     * @param {number} page - Page number
     * @param {number} limit - Items per page
     * @param {Object} options - Request options
     * @returns {Promise<Object>} Keys response
     */
    function fetchKeys(page, limit, options) {
        page = page || 1;
        limit = limit || 20;
        options = options || {};
        return fetchWithTimeout(
            BASE_URL + '/api/keys?page=' + page + '&limit=' + limit,
            {},
            options.timeout
        )
            .then(handleResponse)
            .then(function(data) {
                updateCache('keys', data);
                updateNetworkState(true);
                return data;
            })
            .catch(function(error) {
                updateNetworkState(false, error);
                throw error;
            });
    }

    /**
     * Fetch single key value
     * @param {string} key - Key name
     * @param {Object} options - Request options
     * @returns {Promise<Object>} Key value response
     */
    function fetchKey(key, options) {
        options = options || {};
        return fetchWithTimeout(
            BASE_URL + '/api/keys/' + encodeURIComponent(key),
            {},
            options.timeout
        )
            .then(handleResponse)
            .then(function(data) {
                updateNetworkState(true);
                return data;
            })
            .catch(function(error) {
                updateNetworkState(false, error);
                throw error;
            });
    }

    /**
     * Set a key-value pair
     * @param {string} key - Key name
     * @param {string} value - Value to set
     * @param {number} flags - Optional flags
     * @param {number} ttl - Time to live in seconds
     * @param {Object} options - Request options
     * @returns {Promise<Object>} Set response
     */
    function setKey(key, value, flags, ttl, options) {
        options = options || {};
        return fetchWithTimeout(BASE_URL + '/api/keys', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ key: key, value: value, flags: flags || 0, ttl: ttl || 0 })
        }, options.timeout)
            .then(handleResponse)
            .then(function(data) {
                updateNetworkState(true);
                return data;
            })
            .catch(function(error) {
                updateNetworkState(false, error);
                throw error;
            });
    }

    /**
     * Delete a key
     * @param {string} key - Key name
     * @param {Object} options - Request options
     * @returns {Promise<Object>} Delete response
     */
    function deleteKey(key, options) {
        options = options || {};
        return fetchWithTimeout(
            BASE_URL + '/api/keys/' + encodeURIComponent(key),
            { method: 'DELETE' },
            options.timeout
        )
            .then(handleResponse)
            .then(function(data) {
                updateNetworkState(true);
                return data;
            })
            .catch(function(error) {
                updateNetworkState(false, error);
                throw error;
            });
    }

    /**
     * Fetch server configuration
     * @param {Object} options - Request options
     * @returns {Promise<Object>} Config response
     */
    function fetchConfig(options) {
        options = options || {};
        return fetchWithTimeout(BASE_URL + '/api/config', {}, options.timeout)
            .then(handleResponse)
            .then(function(data) {
                updateCache('config', data);
                updateNetworkState(true);
                return data;
            })
            .catch(function(error) {
                updateNetworkState(false, error);
                throw error;
            });
    }

    /**
     * Handle fetch response
     * @param {Response} response - Fetch response
     * @returns {Promise<Object>} Parsed JSON
     */
    function handleResponse(response) {
        if (!response.ok) {
            if (response.status >= 500) {
                throw new NetworkError(
                    'Server unavailable: Server returned error ' + response.status,
                    ErrorTypes.SERVER_UNAVAILABLE,
                    { status: response.status }
                );
            }
            return response.json().then(function(err) {
                throw new NetworkError(
                    err.message || 'API error: ' + response.status,
                    ErrorTypes.API_ERROR,
                    { status: response.status, response: err }
                );
            }).catch(function(jsonError) {
                if (jsonError instanceof NetworkError) {
                    throw jsonError;
                }
                throw new NetworkError(
                    'API error: ' + response.status,
                    ErrorTypes.API_ERROR,
                    { status: response.status }
                );
            });
        }
        return response.json();
    }

    /**
     * Get user-friendly error message for display
     * @param {Error} error - The error object
     * @returns {string} Human-readable error message
     */
    function getErrorMessage(error) {
        if (error instanceof NetworkError) {
            switch (error.type) {
                case ErrorTypes.TIMEOUT:
                    return 'Connection timeout. The server is taking too long to respond. Please check your network connection and try again.';
                case ErrorTypes.SERVER_UNAVAILABLE:
                    return 'Server unavailable. Unable to reach the MirDB server. Please ensure the server is running.';
                case ErrorTypes.NETWORK_ERROR:
                    return 'Network error. Please check your internet connection and try again.';
                case ErrorTypes.API_ERROR:
                    return error.message || 'An error occurred while processing your request.';
                default:
                    return error.message;
            }
        }
        return error.message || 'An unexpected error occurred.';
    }

    /**
     * Check if error is a network connectivity error
     * @param {Error} error - The error object
     * @returns {boolean} True if network-related error
     */
    function isNetworkError(error) {
        if (error instanceof NetworkError) {
            return error.type === ErrorTypes.TIMEOUT ||
                   error.type === ErrorTypes.SERVER_UNAVAILABLE ||
                   error.type === ErrorTypes.NETWORK_ERROR;
        }
        return false;
    }

    /**
     * Set the API timeout value
     * @param {number} timeout - Timeout in milliseconds
     */
    function setTimeout(timeout) {
        if (typeof timeout === 'number' && timeout > 0) {
            DEFAULT_TIMEOUT_MS = timeout;
        }
    }

    /**
     * Get current timeout value
     * @returns {number} Current timeout in milliseconds
     */
    function getTimeout() {
        return DEFAULT_TIMEOUT_MS;
    }

    return {
        fetchStatus: fetchStatus,
        fetchKeys: fetchKeys,
        fetchKey: fetchKey,
        setKey: setKey,
        deleteKey: deleteKey,
        fetchConfig: fetchConfig,
        // Network error handling exports (Scenario 17)
        onConnectivityChange: onConnectivityChange,
        getNetworkState: getNetworkState,
        getCachedData: getCachedData,
        getErrorMessage: getErrorMessage,
        isNetworkError: isNetworkError,
        setTimeout: setTimeout,
        getTimeout: getTimeout,
        ErrorTypes: ErrorTypes,
        NetworkError: NetworkError
    };
})();
