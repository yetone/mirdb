/**
 * Network Error Handling E2E Tests
 * Owner: Scenario 17 - Error Handling - Network Errors
 *
 * Tests:
 * - TC1: API request timeout - UI shows connection timeout error message
 * - TC2: Server unavailable during refresh - UI shows server unavailable message, retains last known data
 * - TC3: Recovery after network restored - Auto-refresh resumes when connectivity restored
 */

const fs = require('fs');
const path = require('path');
const vm = require('vm');

// Read source files for testing
const apiJsPath = path.join(__dirname, '../src/web/scripts/api.js');
const statusJsPath = path.join(__dirname, '../src/web/scripts/status.js');
const uiJsPath = path.join(__dirname, '../src/web/scripts/ui.js');

const apiJsContent = fs.readFileSync(apiJsPath, 'utf8');
const statusJsContent = fs.readFileSync(statusJsPath, 'utf8');
const uiJsContent = fs.readFileSync(uiJsPath, 'utf8');

/**
 * Helper to load MirDBApi in a sandboxed context
 */
function loadMirDBApi() {
    const sandbox = {
        fetch: global.fetch,
        console: console,
        setTimeout: global.setTimeout,
        clearTimeout: global.clearTimeout,
        AbortController: global.AbortController,
        Promise: Promise,
        Error: Error,
        Object: Object,
        Array: Array,
        Date: Date,
        JSON: JSON,
        encodeURIComponent: encodeURIComponent
    };
    vm.createContext(sandbox);
    vm.runInContext(apiJsContent, sandbox);
    return sandbox.MirDBApi;
}

describe('Network Error Handling - Code Structure Verification', () => {
    describe('API.js Error Handling Features', () => {
        test('api.js should have fetchWithTimeout function', () => {
            expect(apiJsContent).toContain('fetchWithTimeout');
        });

        test('api.js should have NetworkError class', () => {
            expect(apiJsContent).toContain('function NetworkError');
            expect(apiJsContent).toContain('NetworkError.prototype');
        });

        test('api.js should define error types', () => {
            expect(apiJsContent).toContain('ErrorTypes');
            expect(apiJsContent).toContain('TIMEOUT');
            expect(apiJsContent).toContain('SERVER_UNAVAILABLE');
            expect(apiJsContent).toContain('NETWORK_ERROR');
        });

        test('api.js should have network state tracking', () => {
            expect(apiJsContent).toContain('networkState');
            expect(apiJsContent).toContain('isOnline');
            expect(apiJsContent).toContain('consecutiveFailures');
        });

        test('api.js should have data caching for last known data', () => {
            expect(apiJsContent).toContain('dataCache');
            expect(apiJsContent).toContain('getCachedData');
            expect(apiJsContent).toContain('updateCache');
        });

        test('api.js should have connectivity change listeners', () => {
            expect(apiJsContent).toContain('onConnectivityChange');
            expect(apiJsContent).toContain('connectivityListeners');
            expect(apiJsContent).toContain('notifyConnectivityChange');
        });

        test('api.js should expose getNetworkState function', () => {
            expect(apiJsContent).toContain('getNetworkState');
        });

        test('api.js should have getErrorMessage helper', () => {
            expect(apiJsContent).toContain('getErrorMessage');
        });

        test('api.js should have isNetworkError helper', () => {
            expect(apiJsContent).toContain('isNetworkError');
        });

        test('api.js should use AbortController for timeouts', () => {
            expect(apiJsContent).toContain('AbortController');
            expect(apiJsContent).toContain('controller.abort');
        });
    });

    describe('TC1: API Request Timeout Handling', () => {
        test('api.js should have configurable timeout', () => {
            expect(apiJsContent).toContain('DEFAULT_TIMEOUT_MS');
            expect(apiJsContent).toContain('setTimeout');
            expect(apiJsContent).toContain('getTimeout');
        });

        test('api.js should throw NetworkError with TIMEOUT type on timeout', () => {
            expect(apiJsContent).toContain("ErrorTypes.TIMEOUT");
            expect(apiJsContent).toContain("'Connection timeout:");
        });

        test('api.js should provide user-friendly timeout message', () => {
            expect(apiJsContent).toContain('Connection timeout. The server is taking too long to respond');
        });

        test('fetchWithTimeout should handle AbortError', () => {
            expect(apiJsContent).toContain("error.name === 'AbortError'");
        });
    });

    describe('TC2: Server Unavailable Handling', () => {
        test('api.js should detect network fetch failures', () => {
            expect(apiJsContent).toContain("'Failed to fetch'");
            expect(apiJsContent).toContain("'NetworkError'");
        });

        test('api.js should throw NetworkError with SERVER_UNAVAILABLE type', () => {
            expect(apiJsContent).toContain('ErrorTypes.SERVER_UNAVAILABLE');
            expect(apiJsContent).toContain("'Server unavailable:");
        });

        test('api.js should detect 5xx server errors as unavailable', () => {
            expect(apiJsContent).toContain('response.status >= 500');
        });

        test('api.js should provide user-friendly server unavailable message', () => {
            expect(apiJsContent).toContain('Server unavailable. Unable to reach the MirDB server');
        });

        test('api.js should cache status data', () => {
            expect(apiJsContent).toContain("updateCache('status', data)");
        });

        test('api.js should cache keys data', () => {
            expect(apiJsContent).toContain("updateCache('keys', data)");
        });

        test('api.js should cache config data', () => {
            expect(apiJsContent).toContain("updateCache('config', data)");
        });
    });

    describe('TC3: Network Recovery Detection', () => {
        test('api.js should update network state on success', () => {
            expect(apiJsContent).toContain('updateNetworkState(true)');
        });

        test('api.js should update network state on failure', () => {
            expect(apiJsContent).toContain('updateNetworkState(false, error)');
        });

        test('api.js should notify listeners on connectivity change', () => {
            expect(apiJsContent).toContain('wasOnline !== isOnline');
            expect(apiJsContent).toContain('notifyConnectivityChange');
        });

        test('api.js should track lastSuccessTime', () => {
            expect(apiJsContent).toContain('lastSuccessTime');
        });

        test('api.js should reset consecutiveFailures on success', () => {
            expect(apiJsContent).toContain('consecutiveFailures = 0');
        });

        test('api.js should increment consecutiveFailures on error', () => {
            expect(apiJsContent).toContain('consecutiveFailures++');
        });

        test('onConnectivityChange should return unsubscribe function', () => {
            expect(apiJsContent).toContain('return function()');
            expect(apiJsContent).toContain('connectivityListeners.splice');
        });
    });
});

describe('Network Error Handling - Runtime Tests', () => {
    let MirDBApi;
    let originalFetch;
    let mockFetch;

    beforeEach(() => {
        // Reset document
        document.body.innerHTML = '';

        // Store original fetch
        originalFetch = global.fetch;

        // Create a controllable mock fetch
        mockFetch = jest.fn();
        global.fetch = mockFetch;

        // Load MirDBApi fresh for each test
        MirDBApi = loadMirDBApi();
    });

    afterEach(() => {
        global.fetch = originalFetch;
        jest.clearAllTimers();
        jest.useRealTimers();
    });

    describe('TC1: API Request Timeout', () => {
        test('api should have timeout configuration methods', () => {
            expect(typeof MirDBApi.setTimeout).toBe('function');
            expect(typeof MirDBApi.getTimeout).toBe('function');
        });

        test('setTimeout should update timeout value', () => {
            const originalTimeout = MirDBApi.getTimeout();
            MirDBApi.setTimeout(5000);
            expect(MirDBApi.getTimeout()).toBe(5000);
            MirDBApi.setTimeout(originalTimeout);
        });

        test('setTimeout should ignore invalid values', () => {
            const originalTimeout = MirDBApi.getTimeout();
            MirDBApi.setTimeout(-1000);
            expect(MirDBApi.getTimeout()).toBe(originalTimeout);
            MirDBApi.setTimeout('invalid');
            expect(MirDBApi.getTimeout()).toBe(originalTimeout);
        });

        test('getErrorMessage should return user-friendly timeout message', () => {
            const error = new MirDBApi.NetworkError(
                'Connection timeout',
                MirDBApi.ErrorTypes.TIMEOUT,
                {}
            );
            const message = MirDBApi.getErrorMessage(error);
            expect(message).toContain('Connection timeout');
            expect(message).toContain('taking too long');
        });

        test('ErrorTypes should contain TIMEOUT', () => {
            expect(MirDBApi.ErrorTypes.TIMEOUT).toBe('TIMEOUT');
        });
    });

    describe('TC2: Server Unavailable', () => {
        test('fetchStatus should throw SERVER_UNAVAILABLE on network failure', async () => {
            mockFetch.mockRejectedValue(new Error('Failed to fetch'));

            try {
                await MirDBApi.fetchStatus();
                fail('Expected fetchStatus to throw');
            } catch (error) {
                expect(error.type).toBe(MirDBApi.ErrorTypes.SERVER_UNAVAILABLE);
                expect(error.message).toContain('Server unavailable');
            }
        });

        test('fetchStatus should throw SERVER_UNAVAILABLE on 500 error', async () => {
            mockFetch.mockResolvedValue({
                ok: false,
                status: 500,
                json: () => Promise.resolve({ message: 'Internal Server Error' })
            });

            try {
                await MirDBApi.fetchStatus();
                fail('Expected fetchStatus to throw');
            } catch (error) {
                expect(error.type).toBe(MirDBApi.ErrorTypes.SERVER_UNAVAILABLE);
            }
        });

        test('getCachedData should return last known data after failure', async () => {
            // First, successful request
            const mockData = { memory_usage: 1024, active_connections: 5 };
            mockFetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve(mockData)
            });

            await MirDBApi.fetchStatus();

            // Verify data was cached
            const cachedStatus = MirDBApi.getCachedData('status');
            expect(cachedStatus).toBeTruthy();
            expect(cachedStatus.data).toEqual(mockData);

            // Now simulate failure
            mockFetch.mockRejectedValueOnce(new Error('Failed to fetch'));

            try {
                await MirDBApi.fetchStatus();
            } catch (error) {
                // Error expected
            }

            // Cached data should still be available
            const cachedAfterFailure = MirDBApi.getCachedData('status');
            expect(cachedAfterFailure).toBeTruthy();
            expect(cachedAfterFailure.data).toEqual(mockData);
        });

        test('getErrorMessage should return user-friendly server unavailable message', () => {
            const error = new MirDBApi.NetworkError(
                'Server unavailable',
                MirDBApi.ErrorTypes.SERVER_UNAVAILABLE,
                {}
            );
            const message = MirDBApi.getErrorMessage(error);
            expect(message).toContain('Server unavailable');
            expect(message).toContain('Unable to reach');
        });

        test('ErrorTypes should contain SERVER_UNAVAILABLE', () => {
            expect(MirDBApi.ErrorTypes.SERVER_UNAVAILABLE).toBe('SERVER_UNAVAILABLE');
        });
    });

    describe('TC3: Network Recovery', () => {
        test('network state should update to offline on failure', async () => {
            // First request succeeds
            mockFetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({ test: 'data' })
            });
            await MirDBApi.fetchStatus();

            expect(MirDBApi.getNetworkState().isOnline).toBe(true);

            // Second request fails
            mockFetch.mockRejectedValueOnce(new Error('Failed to fetch'));
            try {
                await MirDBApi.fetchStatus();
            } catch (e) {}

            expect(MirDBApi.getNetworkState().isOnline).toBe(false);
            expect(MirDBApi.getNetworkState().consecutiveFailures).toBeGreaterThan(0);
        });

        test('network state should update to online on recovery', async () => {
            // First request fails
            mockFetch.mockRejectedValueOnce(new Error('Failed to fetch'));
            try {
                await MirDBApi.fetchStatus();
            } catch (e) {}

            expect(MirDBApi.getNetworkState().isOnline).toBe(false);

            // Second request succeeds (recovery)
            mockFetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({ test: 'data' })
            });
            await MirDBApi.fetchStatus();

            expect(MirDBApi.getNetworkState().isOnline).toBe(true);
            expect(MirDBApi.getNetworkState().consecutiveFailures).toBe(0);
        });

        test('onConnectivityChange should notify on state change', async () => {
            const listener = jest.fn();
            const unsubscribe = MirDBApi.onConnectivityChange(listener);

            // First request succeeds
            mockFetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({ test: 'data' })
            });
            await MirDBApi.fetchStatus();

            // Fail to trigger state change to offline
            mockFetch.mockRejectedValueOnce(new Error('Failed to fetch'));
            try {
                await MirDBApi.fetchStatus();
            } catch (e) {}

            expect(listener).toHaveBeenCalled();
            expect(listener).toHaveBeenCalledWith(
                expect.objectContaining({
                    isOnline: false
                })
            );

            unsubscribe();
        });

        test('onConnectivityChange should notify on recovery', async () => {
            const listener = jest.fn();

            // First fail
            mockFetch.mockRejectedValueOnce(new Error('Failed to fetch'));
            try {
                await MirDBApi.fetchStatus();
            } catch (e) {}

            // Subscribe after initial failure
            const unsubscribe = MirDBApi.onConnectivityChange(listener);

            // Now succeed (recovery)
            mockFetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({ test: 'data' })
            });
            await MirDBApi.fetchStatus();

            expect(listener).toHaveBeenCalledWith(
                expect.objectContaining({
                    isOnline: true
                })
            );

            unsubscribe();
        });

        test('unsubscribe should remove listener', async () => {
            const listener = jest.fn();
            const unsubscribe = MirDBApi.onConnectivityChange(listener);

            unsubscribe();

            // First succeed to establish online state
            mockFetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({ test: 'data' })
            });
            await MirDBApi.fetchStatus();

            // Then fail to trigger state change
            mockFetch.mockRejectedValueOnce(new Error('Failed to fetch'));
            try {
                await MirDBApi.fetchStatus();
            } catch (e) {}

            // Listener should not be called after unsubscribe
            expect(listener).not.toHaveBeenCalled();
        });
    });

    describe('Error Classification', () => {
        test('isNetworkError should return true for timeout errors', () => {
            const error = new MirDBApi.NetworkError('Timeout', MirDBApi.ErrorTypes.TIMEOUT, {});
            expect(MirDBApi.isNetworkError(error)).toBe(true);
        });

        test('isNetworkError should return true for server unavailable errors', () => {
            const error = new MirDBApi.NetworkError('Server down', MirDBApi.ErrorTypes.SERVER_UNAVAILABLE, {});
            expect(MirDBApi.isNetworkError(error)).toBe(true);
        });

        test('isNetworkError should return true for network errors', () => {
            const error = new MirDBApi.NetworkError('Network failed', MirDBApi.ErrorTypes.NETWORK_ERROR, {});
            expect(MirDBApi.isNetworkError(error)).toBe(true);
        });

        test('isNetworkError should return false for API errors', () => {
            const error = new MirDBApi.NetworkError('API error', MirDBApi.ErrorTypes.API_ERROR, {});
            expect(MirDBApi.isNetworkError(error)).toBe(false);
        });

        test('isNetworkError should return false for regular errors', () => {
            const error = new Error('Regular error');
            expect(MirDBApi.isNetworkError(error)).toBe(false);
        });
    });
});

describe('Integration with Status Dashboard', () => {
    test('status.js should handle errors from MirDBApi', () => {
        expect(statusJsContent).toContain('.catch');
        expect(statusJsContent).toContain('error');
    });

    test('status.js should show error state in UI', () => {
        expect(statusJsContent).toContain('showError');
        expect(statusJsContent).toContain('status-error');
    });

    test('status.js should have retry button for errors', () => {
        expect(statusJsContent).toContain('error-retry-btn');
        expect(statusJsContent).toContain('MirDBStatus.updateStatus');
    });

    test('status.js should preserve refresh functionality for recovery', () => {
        expect(statusJsContent).toContain('startAutoRefresh');
        expect(statusJsContent).toContain('setInterval');
    });
});

describe('UI Integration', () => {
    test('ui.js should have showToast for error messages', () => {
        expect(uiJsContent).toContain('showToast');
        expect(uiJsContent).toContain("'error'");
    });

    test('showToast should support type parameter for toast styling', () => {
        // showToast dynamically creates 'toast toast-{type}' class
        expect(uiJsContent).toContain("'toast toast-' + type");
    });

    test('main.css should have toast-error style', () => {
        const mainCssPath = path.join(__dirname, '../src/web/styles/main.css');
        const mainCssContent = fs.readFileSync(mainCssPath, 'utf8');
        expect(mainCssContent).toContain('.toast-error');
    });
});
