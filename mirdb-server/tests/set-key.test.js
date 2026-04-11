/**
 * Set Key Tests
 * Owner: Scenario 5 - Key Operations - Set Key
 *
 * Tests:
 * - Set Key form UI rendering
 * - Form field validation
 * - Submit functionality
 * - API integration
 */

const fs = require('fs');
const path = require('path');

// Read the HTML file
const htmlPath = path.join(__dirname, '../src/web/index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

// Read the JavaScript files
const uiJsPath = path.join(__dirname, '../src/web/scripts/ui.js');
const uiJsContent = fs.readFileSync(uiJsPath, 'utf8');

const apiJsPath = path.join(__dirname, '../src/web/scripts/api.js');
const apiJsContent = fs.readFileSync(apiJsPath, 'utf8');

const keysJsPath = path.join(__dirname, '../src/web/scripts/keys.js');
const keysJsContent = fs.readFileSync(keysJsPath, 'utf8');

describe('Set Key Form', () => {
    beforeEach(() => {
        // Set up the DOM with the actual HTML
        document.body.innerHTML = htmlContent;

        // Mock fetch for API calls
        global.fetch = jest.fn();

        // Execute the scripts in order
        eval(uiJsContent);
        eval(apiJsContent);
        eval(keysJsContent);
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    describe('Test Case 2: Set Key form UI rendering', () => {
        test('showSetKeyForm creates a modal with form', () => {
            MirDBKeys.showSetKeyForm();

            const modal = document.querySelector('.modal-overlay');
            expect(modal).toBeTruthy();

            const form = document.querySelector('#set-key-form');
            expect(form).toBeTruthy();
        });

        test('Form displays input for key', () => {
            MirDBKeys.showSetKeyForm();

            const keyInput = document.querySelector('#set-key-key');
            expect(keyInput).toBeTruthy();
            expect(keyInput.type).toBe('text');
            expect(keyInput.required).toBe(true);
        });

        test('Form displays textarea for value', () => {
            MirDBKeys.showSetKeyForm();

            const valueInput = document.querySelector('#set-key-value');
            expect(valueInput).toBeTruthy();
            expect(valueInput.tagName.toLowerCase()).toBe('textarea');
        });

        test('Form displays input for flags', () => {
            MirDBKeys.showSetKeyForm();

            const flagsInput = document.querySelector('#set-key-flags');
            expect(flagsInput).toBeTruthy();
            expect(flagsInput.type).toBe('number');
            expect(flagsInput.value).toBe('0');
        });

        test('Form displays input for TTL', () => {
            MirDBKeys.showSetKeyForm();

            const ttlInput = document.querySelector('#set-key-ttl');
            expect(ttlInput).toBeTruthy();
            expect(ttlInput.type).toBe('number');
            expect(ttlInput.value).toBe('0');
        });

        test('Form displays submit button', () => {
            MirDBKeys.showSetKeyForm();

            const submitBtn = document.querySelector('#set-key-submit');
            expect(submitBtn).toBeTruthy();
            expect(submitBtn.type).toBe('submit');
            expect(submitBtn.textContent).toBe('Set Key');
        });

        test('Form has proper modal title', () => {
            MirDBKeys.showSetKeyForm();

            const title = document.querySelector('.modal-title');
            expect(title).toBeTruthy();
            expect(title.textContent).toBe('Set Key');
        });

        test('Form has cancel button', () => {
            MirDBKeys.showSetKeyForm();

            const cancelBtn = document.querySelector('.btn-secondary');
            expect(cancelBtn).toBeTruthy();
            expect(cancelBtn.textContent).toBe('Cancel');
        });

        test('Form inputs have accessible labels', () => {
            MirDBKeys.showSetKeyForm();

            const labels = document.querySelectorAll('.form-label');
            expect(labels.length).toBeGreaterThanOrEqual(4);

            const keyLabel = document.querySelector('label[for="set-key-key"]');
            expect(keyLabel).toBeTruthy();
            expect(keyLabel.textContent).toContain('Key');
        });
    });

    describe('Test Case 3: Submit valid key-value via form', () => {
        test('Submitting form with valid data calls API', async () => {
            // Mock successful API response
            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    success: true,
                    message: 'Key stored successfully',
                    key: 'test-key'
                })
            });

            MirDBKeys.showSetKeyForm();

            // Fill form fields
            const keyInput = document.querySelector('#set-key-key');
            const valueInput = document.querySelector('#set-key-value');
            const flagsInput = document.querySelector('#set-key-flags');
            const ttlInput = document.querySelector('#set-key-ttl');

            keyInput.value = 'test-key';
            valueInput.value = 'test-value';
            flagsInput.value = '0';
            ttlInput.value = '3600';

            // Submit form
            const form = document.querySelector('#set-key-form');
            const submitEvent = new Event('submit', { bubbles: true, cancelable: true });
            form.dispatchEvent(submitEvent);

            // Wait for async operations
            await new Promise(resolve => setTimeout(resolve, 50));

            // Verify API was called
            expect(global.fetch).toHaveBeenCalledWith(
                '/api/keys',
                expect.objectContaining({
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        key: 'test-key',
                        value: 'test-value',
                        flags: 0,
                        ttl: 3600
                    })
                })
            );
        });

        test('Successful submission closes modal', async () => {
            // Mock successful API response
            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    success: true,
                    message: 'Key stored successfully',
                    key: 'test-key'
                })
            });

            MirDBKeys.showSetKeyForm();

            // Fill form
            document.querySelector('#set-key-key').value = 'test-key';
            document.querySelector('#set-key-value').value = 'test-value';

            // Submit form
            const form = document.querySelector('#set-key-form');
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));

            // Wait for async operations
            await new Promise(resolve => setTimeout(resolve, 100));

            // Modal should be closed
            const modal = document.querySelector('.modal-overlay');
            expect(modal).toBeFalsy();
        });

        test('Successful submission shows success toast', async () => {
            // Mock successful API response
            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    success: true,
                    message: 'Key stored successfully',
                    key: 'mykey'
                })
            });

            MirDBKeys.showSetKeyForm();

            // Fill form
            document.querySelector('#set-key-key').value = 'mykey';
            document.querySelector('#set-key-value').value = 'myvalue';

            // Submit form
            const form = document.querySelector('#set-key-form');
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));

            // Wait for async operations
            await new Promise(resolve => setTimeout(resolve, 100));

            // Success toast should be shown
            const toast = document.querySelector('.toast-success');
            expect(toast).toBeTruthy();
            expect(toast.textContent).toContain('mykey');
            expect(toast.textContent).toContain('stored successfully');
        });
    });

    describe('Form Validation', () => {
        test('Empty key shows error', async () => {
            MirDBKeys.showSetKeyForm();

            // Leave key empty
            document.querySelector('#set-key-key').value = '';
            document.querySelector('#set-key-value').value = 'test-value';

            // Submit form
            const form = document.querySelector('#set-key-form');
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));

            // Wait for validation
            await new Promise(resolve => setTimeout(resolve, 50));

            // Error toast should be shown
            const toast = document.querySelector('.toast-error');
            expect(toast).toBeTruthy();
            expect(toast.textContent).toContain('required');
        });

        test('Whitespace-only key shows error', async () => {
            MirDBKeys.showSetKeyForm();

            // Set whitespace-only key
            document.querySelector('#set-key-key').value = '   ';
            document.querySelector('#set-key-value').value = 'test-value';

            // Submit form
            const form = document.querySelector('#set-key-form');
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));

            // Wait for validation
            await new Promise(resolve => setTimeout(resolve, 50));

            // API should not be called
            expect(global.fetch).not.toHaveBeenCalled();
        });

        test('Empty value is allowed', async () => {
            // Mock successful API response
            global.fetch.mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({
                    success: true,
                    message: 'Key stored successfully',
                    key: 'test-key'
                })
            });

            MirDBKeys.showSetKeyForm();

            // Set key with empty value
            document.querySelector('#set-key-key').value = 'test-key';
            document.querySelector('#set-key-value').value = '';

            // Submit form
            const form = document.querySelector('#set-key-form');
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));

            // Wait for async operations
            await new Promise(resolve => setTimeout(resolve, 50));

            // API should be called
            expect(global.fetch).toHaveBeenCalled();
        });
    });

    describe('Error Handling', () => {
        test('API error shows error toast', async () => {
            // Mock API error response
            global.fetch.mockResolvedValueOnce({
                ok: false,
                json: () => Promise.resolve({
                    error: 'bad_request',
                    message: 'Invalid key name'
                })
            });

            MirDBKeys.showSetKeyForm();

            // Fill form
            document.querySelector('#set-key-key').value = 'test-key';
            document.querySelector('#set-key-value').value = 'test-value';

            // Submit form
            const form = document.querySelector('#set-key-form');
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));

            // Wait for async operations
            await new Promise(resolve => setTimeout(resolve, 100));

            // Error toast should be shown
            const toast = document.querySelector('.toast-error');
            expect(toast).toBeTruthy();
        });

        test('API error keeps modal open', async () => {
            // Mock API error response
            global.fetch.mockResolvedValueOnce({
                ok: false,
                json: () => Promise.resolve({
                    error: 'bad_request',
                    message: 'Invalid key name'
                })
            });

            MirDBKeys.showSetKeyForm();

            // Fill form
            document.querySelector('#set-key-key').value = 'test-key';
            document.querySelector('#set-key-value').value = 'test-value';

            // Submit form
            const form = document.querySelector('#set-key-form');
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));

            // Wait for async operations
            await new Promise(resolve => setTimeout(resolve, 100));

            // Modal should still be open
            const modal = document.querySelector('.modal-overlay');
            expect(modal).toBeTruthy();
        });

        test('Submit button shows loading state during API call', async () => {
            // Mock slow API response
            global.fetch.mockImplementation(() => new Promise(resolve => {
                setTimeout(() => resolve({
                    ok: true,
                    json: () => Promise.resolve({ success: true })
                }), 200);
            }));

            MirDBKeys.showSetKeyForm();

            // Fill form
            document.querySelector('#set-key-key').value = 'test-key';
            document.querySelector('#set-key-value').value = 'test-value';

            const submitBtn = document.querySelector('#set-key-submit');
            expect(submitBtn.disabled).toBe(false);

            // Submit form
            const form = document.querySelector('#set-key-form');
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));

            // Check loading state immediately after submit
            await new Promise(resolve => setTimeout(resolve, 10));
            expect(submitBtn.disabled).toBe(true);
            expect(submitBtn.textContent).toBe('Setting...');
        });
    });

    describe('Cancel Button', () => {
        test('Cancel button closes modal', () => {
            MirDBKeys.showSetKeyForm();

            const cancelBtn = document.querySelector('.btn-secondary');
            cancelBtn.click();

            const modal = document.querySelector('.modal-overlay');
            expect(modal).toBeFalsy();
        });

        test('Cancel button does not submit form', () => {
            MirDBKeys.showSetKeyForm();

            // Fill form
            document.querySelector('#set-key-key').value = 'test-key';
            document.querySelector('#set-key-value').value = 'test-value';

            // Click cancel
            const cancelBtn = document.querySelector('.btn-secondary');
            cancelBtn.click();

            // API should not be called
            expect(global.fetch).not.toHaveBeenCalled();
        });
    });
});

describe('MirDBKeys Module', () => {
    beforeEach(() => {
        document.body.innerHTML = htmlContent;
        eval(uiJsContent);
        eval(apiJsContent);
        eval(keysJsContent);
    });

    test('MirDBKeys namespace is available', () => {
        expect(window.MirDBKeys).toBeDefined();
        expect(typeof MirDBKeys.showSetKeyForm).toBe('function');
    });

    test('showSetKeyForm is exported', () => {
        expect(typeof MirDBKeys.showSetKeyForm).toBe('function');
    });
});
