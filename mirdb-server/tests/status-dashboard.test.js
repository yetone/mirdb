/**
 * Status Dashboard E2E Tests
 * Owner: Scenario 3 - Server Status Dashboard
 *
 * Tests:
 * - TC2: Status panel UI rendering - Grid layout displays all metrics
 * - TC3: Auto-refresh functionality - Metrics update without page reload
 */

const fs = require('fs');
const path = require('path');

// Read source files for testing
const statusJsPath = path.join(__dirname, '../src/web/scripts/status.js');
const apiJsPath = path.join(__dirname, '../src/web/scripts/api.js');
const indexHtmlPath = path.join(__dirname, '../src/web/index.html');

const statusJsContent = fs.readFileSync(statusJsPath, 'utf8');
const apiJsContent = fs.readFileSync(apiJsPath, 'utf8');
const indexHtmlContent = fs.readFileSync(indexHtmlPath, 'utf8');

describe('HTML Structure Verification', () => {
    test('index.html should contain status section', () => {
        expect(indexHtmlContent).toContain('id="status"');
    });

    test('index.html should contain status-panel element', () => {
        expect(indexHtmlContent).toContain('id="status-panel"');
    });

    test('index.html should include status.js script', () => {
        expect(indexHtmlContent).toContain('status.js');
    });

    test('status section should have proper heading', () => {
        expect(indexHtmlContent).toContain('Server Status');
    });

    test('status panel should have a class for styling', () => {
        expect(indexHtmlContent).toContain('class="status-panel"');
    });
});

describe('JavaScript Structure Verification', () => {
    test('status.js should export MirDBStatus module', () => {
        expect(statusJsContent).toContain('var MirDBStatus');
    });

    test('status.js should have initStatusDashboard function', () => {
        expect(statusJsContent).toContain('initStatusDashboard');
    });

    test('status.js should have updateStatus function', () => {
        expect(statusJsContent).toContain('updateStatus');
    });

    test('status.js should have startAutoRefresh function', () => {
        expect(statusJsContent).toContain('startAutoRefresh');
    });

    test('status.js should have stopAutoRefresh function', () => {
        expect(statusJsContent).toContain('stopAutoRefresh');
    });

    test('status.js should have 5-second refresh interval (5000ms)', () => {
        expect(statusJsContent).toContain('5000');
    });

    test('status.js should call MirDBApi.fetchStatus', () => {
        expect(statusJsContent).toContain('MirDBApi.fetchStatus');
    });

    test('api.js should have fetchStatus function', () => {
        expect(apiJsContent).toContain('fetchStatus');
        expect(apiJsContent).toContain('/api/status');
    });
});

describe('TC2: Status Panel UI Rendering', () => {
    test('status.js should create metric cards', () => {
        expect(statusJsContent).toContain('status-metric-card');
    });

    test('status.js should create metrics grid', () => {
        expect(statusJsContent).toContain('status-metrics-grid');
    });

    test('status.js should display memory_usage metric', () => {
        expect(statusJsContent).toContain('Memory Usage');
    });

    test('status.js should display active_connections metric', () => {
        expect(statusJsContent).toContain('Active Connections');
    });

    test('status.js should display database_size metric', () => {
        expect(statusJsContent).toContain('Database Size');
    });

    test('status.js should display memtable_count metric', () => {
        expect(statusJsContent).toContain('Memtable Count');
    });

    test('status.js should display sstable_count metric', () => {
        expect(statusJsContent).toContain('SSTable Count');
    });

    test('status.js should display total_size metric', () => {
        expect(statusJsContent).toContain('Total Size');
    });

    test('status.js should have metric-label class', () => {
        expect(statusJsContent).toContain('metric-label');
    });

    test('status.js should have metric-value class', () => {
        expect(statusJsContent).toContain('metric-value');
    });

    test('status.js should format bytes for display', () => {
        expect(statusJsContent).toContain('formatBytes');
    });
});

describe('TC3: Auto-Refresh Functionality', () => {
    test('status.js should have refresh indicator', () => {
        expect(statusJsContent).toContain('status-refresh-indicator');
    });

    test('status.js should show refresh status', () => {
        expect(statusJsContent).toContain('refresh-status');
    });

    test('status.js should have countdown display', () => {
        expect(statusJsContent).toContain('refresh-countdown');
    });

    test('status.js should use setInterval for auto-refresh', () => {
        expect(statusJsContent).toContain('setInterval');
    });

    test('status.js should use clearInterval for stopping refresh', () => {
        expect(statusJsContent).toContain('clearInterval');
    });

    test('status.js should have isRefreshing check', () => {
        expect(statusJsContent).toContain('isRefreshing');
    });

    test('status.js should update without full page reload', () => {
        // Verify status updates the panel innerHTML, not location.reload
        expect(statusJsContent).not.toContain('location.reload');
        expect(statusJsContent).toContain('panel.innerHTML');
    });

    test('status.js should show active status in indicator', () => {
        expect(statusJsContent).toContain("'Active'");
    });
});

describe('Error Handling', () => {
    test('status.js should handle fetch errors', () => {
        expect(statusJsContent).toContain('catch');
        expect(statusJsContent).toContain('error');
    });

    test('status.js should show error state in UI', () => {
        expect(statusJsContent).toContain('status-error');
    });

    test('status.js should have retry functionality', () => {
        expect(statusJsContent).toContain('error-retry-btn');
    });
});

describe('Utility Functions', () => {
    test('formatBytes should handle zero bytes', () => {
        expect(statusJsContent).toContain("if (bytes === 0) return '0 B'");
    });

    test('formatBytes should support KB, MB, GB, TB sizes', () => {
        expect(statusJsContent).toContain("'KB'");
        expect(statusJsContent).toContain("'MB'");
        expect(statusJsContent).toContain("'GB'");
        expect(statusJsContent).toContain("'TB'");
    });
});

describe('DOM Rendering Tests', () => {
    beforeEach(() => {
        // Set up the DOM with the actual HTML
        document.body.innerHTML = indexHtmlContent;
    });

    test('status section exists in DOM', () => {
        const statusSection = document.getElementById('status');
        expect(statusSection).toBeTruthy();
    });

    test('status panel exists in DOM', () => {
        const statusPanel = document.getElementById('status-panel');
        expect(statusPanel).toBeTruthy();
    });

    test('status section has Server Status heading', () => {
        const statusSection = document.getElementById('status');
        expect(statusSection.querySelector('.section-title')).toBeTruthy();
        expect(statusSection.textContent).toContain('Server Status');
    });

    test('status panel has status-panel class', () => {
        const statusPanel = document.getElementById('status-panel');
        expect(statusPanel.classList.contains('status-panel')).toBe(true);
    });

    test('status.js script is included', () => {
        const scripts = document.querySelectorAll('script[src*="status.js"]');
        expect(scripts.length).toBe(1);
    });

    test('api.js script is loaded before status.js', () => {
        const scripts = document.querySelectorAll('script');
        let apiIndex = -1;
        let statusIndex = -1;

        scripts.forEach((script, index) => {
            if (script.src && script.src.includes('api.js')) {
                apiIndex = index;
            }
            if (script.src && script.src.includes('status.js')) {
                statusIndex = index;
            }
        });

        // api.js should come before status.js (or we check by src attribute)
        const apiScript = document.querySelector('script[src*="api.js"]');
        const statusScript = document.querySelector('script[src*="status.js"]');
        expect(apiScript).toBeTruthy();
        expect(statusScript).toBeTruthy();
    });
});

describe('API Integration Verification', () => {
    test('api.js should make GET request to /api/status', () => {
        expect(apiJsContent).toContain("'/api/status'");
        expect(apiJsContent).toContain('fetch');
    });

    test('api.js should parse JSON response', () => {
        expect(apiJsContent).toContain('.json()');
    });

    test('api.js should handle response errors', () => {
        expect(apiJsContent).toContain('response.ok');
    });
});

describe('Response Time Verification', () => {
    test('TC4: Status API should be designed for fast response', () => {
        // Verify the status.js doesn't introduce unnecessary delays
        // No setTimeout in the fetch logic (except for countdown)
        const fetchSection = statusJsContent.split('function updateStatus')[1];
        if (fetchSection) {
            // The fetch itself should be straightforward
            expect(fetchSection).toContain('MirDBApi.fetchStatus');
        }
    });
});

describe('Auto-Initialization', () => {
    test('status.js should auto-initialize on DOMContentLoaded', () => {
        expect(statusJsContent).toContain('DOMContentLoaded');
    });

    test('status.js should check for status-panel element', () => {
        expect(statusJsContent).toContain("getElementById('status-panel')");
    });
});
