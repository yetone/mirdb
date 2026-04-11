/**
 * Key Browser UI Tests
 * Owner: Scenario 4 - Key Browser View Keys
 *
 * Tests:
 * - Table displays columns: Key, Size, TTL, Actions
 * - Search input filters displayed keys
 * - Click key row opens modal with value and copy button
 */

const fs = require('fs');
const path = require('path');
const vm = require('vm');

// Load the JavaScript files
const uiJsPath = path.join(__dirname, '../src/web/scripts/ui.js');
const apiJsPath = path.join(__dirname, '../src/web/scripts/api.js');
const keysJsPath = path.join(__dirname, '../src/web/scripts/keys.js');

const uiCode = fs.readFileSync(uiJsPath, 'utf8');
const apiCode = fs.readFileSync(apiJsPath, 'utf8');
const keysCode = fs.readFileSync(keysJsPath, 'utf8');

describe('Key Browser UI', () => {
    beforeEach(() => {
        // Reset DOM
        document.body.innerHTML = `
            <section id="keys" class="section keys-section">
                <h2 class="section-title">Key Browser</h2>
                <div id="key-browser" class="key-browser">
                    <div class="keys-placeholder">Loading keys...</div>
                </div>
            </section>
        `;

        // Create sandbox context with DOM globals
        const sandbox = {
            window: global.window,
            document: global.document,
            navigator: global.navigator,
            console: console,
            setTimeout: setTimeout,
            clearTimeout: clearTimeout,
            fetch: jest.fn(),
            Promise: Promise
        };

        // Execute the JS code using vm.runInNewContext to avoid global pollution
        vm.runInNewContext(uiCode, sandbox);
        vm.runInNewContext(apiCode, sandbox);
        vm.runInNewContext(keysCode, sandbox);

        // Copy the modules to global for test access
        global.MirDBUI = sandbox.MirDBUI;
        global.MirDBApi = sandbox.MirDBApi;
        global.MirDBKeys = sandbox.MirDBKeys;
        global.fetch = sandbox.fetch;
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    describe('Key Table Rendering', () => {
        test('Table displays columns: Key, Size, TTL, Actions', async () => {
            // Mock API response
            const mockKeys = [
                { key: 'user:123', size: 256, ttl: 3600 },
                { key: 'session:abc', size: 1024, ttl: 0 },
                { key: 'cache:config', size: 512, ttl: 86400 }
            ];

            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    keys: mockKeys,
                    page: 1,
                    limit: 10,
                    total: 3
                })
            });

            // Initialize key browser
            global.global.MirDBKeys.initKeyBrowser();

            // Wait for async operations
            await new Promise(resolve => setTimeout(resolve, 100));

            // Check table headers
            const table = document.querySelector('.key-table');
            expect(table).not.toBeNull();

            const headers = table.querySelectorAll('th');
            const headerTexts = Array.from(headers).map(h => h.textContent);

            expect(headerTexts).toContain('Key');
            expect(headerTexts).toContain('Size');
            expect(headerTexts).toContain('TTL');
            expect(headerTexts).toContain('Actions');
        });

        test('Table displays key data correctly', async () => {
            const mockKeys = [
                { key: 'test_key', size: 100, ttl: 3600 }
            ];

            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    keys: mockKeys,
                    page: 1,
                    limit: 10,
                    total: 1
                })
            });

            global.MirDBKeys.initKeyBrowser();
            await new Promise(resolve => setTimeout(resolve, 100));

            const rows = document.querySelectorAll('.key-table tbody tr');
            expect(rows.length).toBe(1);

            const row = rows[0];
            expect(row.getAttribute('data-key')).toBe('test_key');

            const cells = row.querySelectorAll('td');
            expect(cells[0].textContent).toBe('test_key'); // Key
            expect(cells[1].textContent).toContain('100'); // Size (formatted)
            expect(cells[2].textContent).toContain('3600'); // TTL
        });

        test('Empty state is shown when no keys exist', async () => {
            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    keys: [],
                    page: 1,
                    limit: 10,
                    total: 0
                })
            });

            global.MirDBKeys.initKeyBrowser();
            await new Promise(resolve => setTimeout(resolve, 100));

            const emptyState = document.querySelector('.empty-state');
            expect(emptyState).not.toBeNull();
            expect(emptyState.textContent).toContain('No keys found');
        });
    });

    describe('Search Filtering', () => {
        test('Search input filters displayed keys', async () => {
            const mockKeys = [
                { key: 'user:123', size: 100, ttl: 0 },
                { key: 'user:456', size: 100, ttl: 0 },
                { key: 'session:abc', size: 100, ttl: 0 }
            ];

            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    keys: mockKeys,
                    page: 1,
                    limit: 10,
                    total: 3
                })
            });

            global.MirDBKeys.initKeyBrowser();
            await new Promise(resolve => setTimeout(resolve, 100));

            // Initially all 3 keys should be shown
            let rows = document.querySelectorAll('.key-table tbody tr');
            expect(rows.length).toBe(3);

            // Filter by "user"
            global.MirDBKeys.filterKeys('user');
            await new Promise(resolve => setTimeout(resolve, 50));

            rows = document.querySelectorAll('.key-table tbody tr');
            expect(rows.length).toBe(2);

            // Check that only user keys are shown
            const keyNames = Array.from(rows).map(r => r.getAttribute('data-key'));
            expect(keyNames).toContain('user:123');
            expect(keyNames).toContain('user:456');
            expect(keyNames).not.toContain('session:abc');
        });

        test('Search input exists and is focusable', async () => {
            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({ keys: [], page: 1, limit: 10, total: 0 })
            });

            global.MirDBKeys.initKeyBrowser();
            await new Promise(resolve => setTimeout(resolve, 100));

            const searchInput = document.getElementById('key-search-input');
            expect(searchInput).not.toBeNull();
            expect(searchInput.getAttribute('aria-label')).toBe('Search keys');
            expect(searchInput.placeholder).toContain('Search');
        });

        test('Empty filter shows all keys', async () => {
            const mockKeys = [
                { key: 'key1', size: 100, ttl: 0 },
                { key: 'key2', size: 100, ttl: 0 }
            ];

            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    keys: mockKeys,
                    page: 1,
                    limit: 10,
                    total: 2
                })
            });

            global.MirDBKeys.initKeyBrowser();
            await new Promise(resolve => setTimeout(resolve, 100));

            // Filter then clear
            global.MirDBKeys.filterKeys('nonexistent');
            await new Promise(resolve => setTimeout(resolve, 50));

            global.MirDBKeys.filterKeys('');
            await new Promise(resolve => setTimeout(resolve, 50));

            const rows = document.querySelectorAll('.key-table tbody tr');
            expect(rows.length).toBe(2);
        });
    });

    describe('Key Value Modal', () => {
        test('Click key row opens modal with value and copy button', async () => {
            const mockKeys = [
                { key: 'clickable_key', size: 100, ttl: 0 }
            ];

            global.fetch
                .mockResolvedValueOnce({
                    ok: true,
                    json: () => Promise.resolve({
                        keys: mockKeys,
                        page: 1,
                        limit: 10,
                        total: 1
                    })
                })
                .mockResolvedValueOnce({
                    ok: true,
                    json: () => Promise.resolve({
                        key: 'clickable_key',
                        value: 'test_value_content',
                        flags: 0,
                        ttl: 0,
                        size: 18
                    })
                });

            global.MirDBKeys.initKeyBrowser();
            await new Promise(resolve => setTimeout(resolve, 100));

            // Click the View button
            const viewBtn = document.querySelector('.view-key-btn');
            expect(viewBtn).not.toBeNull();
            viewBtn.click();

            // Wait for modal to appear
            await new Promise(resolve => setTimeout(resolve, 100));

            // Check modal exists
            const modal = document.querySelector('.modal-overlay');
            expect(modal).not.toBeNull();

            // Check modal has copy button
            const copyBtn = document.querySelector('.copy-value-btn');
            expect(copyBtn).not.toBeNull();
            expect(copyBtn.getAttribute('aria-label')).toContain('clipboard');
        });

        test('Modal displays key value content', async () => {
            // Setup mock for key fetch
            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    key: 'display_key',
                    value: 'displayed_value',
                    flags: 42,
                    ttl: 3600,
                    size: 14
                })
            });

            // Call showKeyValue directly
            global.MirDBKeys.showKeyValue('display_key');
            await new Promise(resolve => setTimeout(resolve, 100));

            // Check value is displayed
            const valueContent = document.querySelector('.value-content');
            expect(valueContent).not.toBeNull();
            expect(valueContent.textContent).toBe('displayed_value');
        });

        test('Modal has close button that works', async () => {
            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    key: 'test',
                    value: 'value',
                    flags: 0,
                    ttl: 0,
                    size: 5
                })
            });

            global.MirDBKeys.showKeyValue('test');
            await new Promise(resolve => setTimeout(resolve, 100));

            // Modal should exist
            expect(document.querySelector('.modal-overlay')).not.toBeNull();

            // Click close button
            const closeBtn = document.querySelector('.modal-close');
            expect(closeBtn).not.toBeNull();
            closeBtn.click();

            // Modal should be gone
            expect(document.querySelector('.modal-overlay')).toBeNull();
        });
    });

    describe('Pagination', () => {
        test('Pagination controls appear when there are multiple pages', async () => {
            const mockKeys = Array(10).fill(null).map((_, i) => ({
                key: `key_${i}`,
                size: 100,
                ttl: 0
            }));

            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    keys: mockKeys,
                    page: 1,
                    limit: 10,
                    total: 25  // More than one page
                })
            });

            global.MirDBKeys.initKeyBrowser();
            await new Promise(resolve => setTimeout(resolve, 100));

            const pagination = document.getElementById('key-pagination');
            expect(pagination).not.toBeNull();
            expect(pagination.innerHTML).not.toBe('');

            // Should have prev/next buttons
            const buttons = pagination.querySelectorAll('button');
            expect(buttons.length).toBeGreaterThanOrEqual(2);

            // Page info should be visible
            const pageInfo = pagination.querySelector('.pagination-info');
            expect(pageInfo).not.toBeNull();
            expect(pageInfo.textContent).toContain('Page');
        });

        test('Pagination is hidden when only one page exists', async () => {
            const mockKeys = [
                { key: 'only_key', size: 100, ttl: 0 }
            ];

            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    keys: mockKeys,
                    page: 1,
                    limit: 10,
                    total: 1  // Only one page
                })
            });

            global.MirDBKeys.initKeyBrowser();
            await new Promise(resolve => setTimeout(resolve, 100));

            const pagination = document.getElementById('key-pagination');
            expect(pagination.innerHTML).toBe('');
        });
    });

    describe('Accessibility', () => {
        test('Table has proper ARIA attributes', async () => {
            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    keys: [{ key: 'k1', size: 100, ttl: 0 }],
                    page: 1,
                    limit: 10,
                    total: 1
                })
            });

            global.MirDBKeys.initKeyBrowser();
            await new Promise(resolve => setTimeout(resolve, 100));

            const table = document.querySelector('.key-table');
            expect(table.getAttribute('role')).toBe('grid');

            const headers = table.querySelectorAll('th');
            headers.forEach(th => {
                expect(th.getAttribute('scope')).toBe('col');
            });
        });

        test('Buttons have aria-labels', async () => {
            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    keys: [{ key: 'accessible_key', size: 100, ttl: 0 }],
                    page: 1,
                    limit: 10,
                    total: 1
                })
            });

            global.MirDBKeys.initKeyBrowser();
            await new Promise(resolve => setTimeout(resolve, 100));

            const viewBtn = document.querySelector('.view-key-btn');
            expect(viewBtn.getAttribute('aria-label')).toContain('accessible_key');

            const deleteBtn = document.querySelector('.delete-key-btn');
            expect(deleteBtn.getAttribute('aria-label')).toContain('accessible_key');
        });
    });
});
