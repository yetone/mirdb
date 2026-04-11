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
 */

var MirDBApi = (function() {
    'use strict';

    var BASE_URL = '';

    /**
     * Fetch server status
     * @returns {Promise<Object>} Status response
     */
    function fetchStatus() {
        return fetch(BASE_URL + '/api/status')
            .then(handleResponse);
    }

    /**
     * Fetch paginated key list
     * @param {number} page - Page number
     * @param {number} limit - Items per page
     * @returns {Promise<Object>} Keys response
     */
    function fetchKeys(page, limit) {
        page = page || 1;
        limit = limit || 20;
        return fetch(BASE_URL + '/api/keys?page=' + page + '&limit=' + limit)
            .then(handleResponse);
    }

    /**
     * Fetch single key value
     * @param {string} key - Key name
     * @returns {Promise<Object>} Key value response
     */
    function fetchKey(key) {
        return fetch(BASE_URL + '/api/keys/' + encodeURIComponent(key))
            .then(handleResponse);
    }

    /**
     * Set a key-value pair
     * @param {string} key - Key name
     * @param {string} value - Value to set
     * @param {number} flags - Optional flags
     * @param {number} ttl - Time to live in seconds
     * @returns {Promise<Object>} Set response
     */
    function setKey(key, value, flags, ttl) {
        return fetch(BASE_URL + '/api/keys', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ key: key, value: value, flags: flags || 0, ttl: ttl || 0 })
        }).then(handleResponse);
    }

    /**
     * Delete a key
     * @param {string} key - Key name
     * @returns {Promise<Object>} Delete response
     */
    function deleteKey(key) {
        return fetch(BASE_URL + '/api/keys/' + encodeURIComponent(key), {
            method: 'DELETE'
        }).then(handleResponse);
    }

    /**
     * Fetch server configuration
     * @returns {Promise<Object>} Config response
     */
    function fetchConfig() {
        return fetch(BASE_URL + '/api/config')
            .then(handleResponse);
    }

    /**
     * Handle fetch response
     * @param {Response} response - Fetch response
     * @returns {Promise<Object>} Parsed JSON
     */
    function handleResponse(response) {
        if (!response.ok) {
            return response.json().then(function(err) {
                throw new Error(err.message || 'API error: ' + response.status);
            });
        }
        return response.json();
    }

    return {
        fetchStatus: fetchStatus,
        fetchKeys: fetchKeys,
        fetchKey: fetchKey,
        setKey: setKey,
        deleteKey: deleteKey,
        fetchConfig: fetchConfig
    };
})();
