/**
 * Quick Start Section Tests
 * Owner: Scenario 2 - Quick Start Section
 *
 * Tests:
 * - Quick Start section content and visibility
 * - Connection example format validation
 * - Copy button functionality
 */

const fs = require('fs');
const path = require('path');

// Read the HTML file
const htmlPath = path.join(__dirname, '../src/web/index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

// Read the JavaScript files
const uiJsPath = path.join(__dirname, '../src/web/scripts/ui.js');
const uiJsContent = fs.readFileSync(uiJsPath, 'utf8');

describe('Quick Start Section', () => {
    beforeEach(() => {
        // Set up the DOM with the actual HTML
        document.body.innerHTML = htmlContent;

        // Execute the UI script
        eval(uiJsContent);
    });

    describe('Test Case 1: Quick Start section content check', () => {
        test('Quick Start section exists and is visible', () => {
            const quickStartSection = document.getElementById('quick-start');
            expect(quickStartSection).toBeTruthy();
            expect(quickStartSection.classList.contains('quick-start')).toBe(true);
        });

        test('Quick Start section has a title', () => {
            const title = document.getElementById('quick-start-title');
            expect(title).toBeTruthy();
            expect(title.textContent).toBe('Quick Start');
        });

        test('Quick Start section displays memcached protocol connection examples', () => {
            const connectCmd = document.getElementById('connect-cmd');
            expect(connectCmd).toBeTruthy();
            expect(connectCmd.textContent).toContain('telnet');
            expect(connectCmd.textContent).toContain('localhost');
            expect(connectCmd.textContent).toContain('12333');
        });

        test('Quick Start section includes netcat alternative', () => {
            const ncCmd = document.getElementById('nc-cmd');
            expect(ncCmd).toBeTruthy();
            expect(ncCmd.textContent).toContain('nc');
            expect(ncCmd.textContent).toContain('localhost');
            expect(ncCmd.textContent).toContain('12333');
        });

        test('Quick Start section includes set command example', () => {
            const setCmd = document.getElementById('set-cmd');
            expect(setCmd).toBeTruthy();
            expect(setCmd.textContent).toContain('set');
        });

        test('Quick Start section includes get command example', () => {
            const getCmd = document.getElementById('get-cmd');
            expect(getCmd).toBeTruthy();
            expect(getCmd.textContent).toContain('get');
        });

        test('Quick Start section includes delete command example', () => {
            const deleteCmd = document.getElementById('delete-cmd');
            expect(deleteCmd).toBeTruthy();
            expect(deleteCmd.textContent).toContain('delete');
        });

        test('Quick Start section has proper ARIA attributes for accessibility', () => {
            const quickStartSection = document.getElementById('quick-start');
            expect(quickStartSection.getAttribute('aria-labelledby')).toBe('quick-start-title');
        });
    });

    describe('Test Case 2: Connection example format validation', () => {
        test('Telnet connection command has correct format', () => {
            const connectCmd = document.getElementById('connect-cmd');
            const command = connectCmd.textContent.trim();

            // Format: telnet <host> <port>
            expect(command).toMatch(/^telnet\s+\w+\s+\d+$/);
        });

        test('Netcat connection command has correct format', () => {
            const ncCmd = document.getElementById('nc-cmd');
            const command = ncCmd.textContent.trim();

            // Format: nc <host> <port>
            expect(command).toMatch(/^nc\s+\w+\s+\d+$/);
        });

        test('Set command has correct memcached syntax', () => {
            const setCmd = document.getElementById('set-cmd');
            const command = setCmd.textContent.trim();

            // Format: set <key> <flags> <exptime> <bytes>\r\n<value>
            expect(command).toMatch(/^set\s+\w+\s+\d+\s+\d+\s+\d+/);
        });

        test('Get command has correct memcached syntax', () => {
            const getCmd = document.getElementById('get-cmd');
            const command = getCmd.textContent.trim();

            // Format: get <key>
            expect(command).toMatch(/^get\s+\w+$/);
        });

        test('Delete command has correct memcached syntax', () => {
            const deleteCmd = document.getElementById('delete-cmd');
            const command = deleteCmd.textContent.trim();

            // Format: delete <key>
            expect(command).toMatch(/^delete\s+\w+$/);
        });

        test('Port number is valid (default 12333)', () => {
            const connectCmd = document.getElementById('connect-cmd');
            const portMatch = connectCmd.textContent.match(/\d{4,5}/);
            expect(portMatch).toBeTruthy();
            const port = parseInt(portMatch[0], 10);
            expect(port).toBeGreaterThanOrEqual(1024);
            expect(port).toBeLessThanOrEqual(65535);
        });
    });

    describe('Test Case 3: Copy button functionality', () => {
        test('Copy buttons exist for each code example', () => {
            const copyButtons = document.querySelectorAll('.copy-btn[data-copy-target]');
            expect(copyButtons.length).toBeGreaterThanOrEqual(5);
        });

        test('Each copy button has data-copy-target attribute', () => {
            const copyButtons = document.querySelectorAll('.copy-btn');
            copyButtons.forEach(button => {
                expect(button.hasAttribute('data-copy-target')).toBe(true);
            });
        });

        test('Copy buttons have accessible labels', () => {
            const copyButtons = document.querySelectorAll('.copy-btn');
            copyButtons.forEach(button => {
                expect(button.hasAttribute('aria-label')).toBe(true);
            });
        });

        test('Copy button targets exist in DOM', () => {
            const copyButtons = document.querySelectorAll('.copy-btn[data-copy-target]');
            copyButtons.forEach(button => {
                const targetId = button.getAttribute('data-copy-target');
                const targetElement = document.getElementById(targetId);
                expect(targetElement).toBeTruthy();
            });
        });

        test('copyToClipboard function copies text correctly', async () => {
            const testText = 'telnet localhost 12333';
            const result = await window.UI.copyToClipboard(testText);

            expect(result).toBe(true);
            expect(navigator.clipboard.writeText).toHaveBeenCalledWith(testText);
        });

        test('Copy button click triggers clipboard copy', async () => {
            // Initialize copy buttons
            window.UI.initCopyButtons();

            const copyButton = document.querySelector('.copy-btn[data-copy-target="connect-cmd"]');
            expect(copyButton).toBeTruthy();

            // Simulate click
            const clickEvent = new MouseEvent('click', {
                bubbles: true,
                cancelable: true,
                view: window
            });

            copyButton.dispatchEvent(clickEvent);

            // Wait for async operation
            await new Promise(resolve => setTimeout(resolve, 10));

            expect(navigator.clipboard.writeText).toHaveBeenCalledWith('telnet localhost 12333');
        });

        test('Copy button shows feedback after successful copy', async () => {
            window.UI.initCopyButtons();

            const copyButton = document.querySelector('.copy-btn[data-copy-target="connect-cmd"]');
            const copyText = copyButton.querySelector('.copy-text');

            expect(copyText.textContent).toBe('Copy');

            // Simulate click
            copyButton.click();

            // Wait for async operation
            await new Promise(resolve => setTimeout(resolve, 50));

            expect(copyButton.classList.contains('copied')).toBe(true);
            expect(copyText.textContent).toBe('Copied!');
        });
    });
});

describe('UI Utilities', () => {
    beforeEach(() => {
        document.body.innerHTML = '';
        eval(uiJsContent);
    });

    test('UI namespace is available on window', () => {
        expect(window.UI).toBeDefined();
        expect(typeof window.UI.copyToClipboard).toBe('function');
        expect(typeof window.UI.initCopyButtons).toBe('function');
        expect(typeof window.UI.showToast).toBe('function');
        expect(typeof window.UI.showModal).toBe('function');
        expect(typeof window.UI.hideModal).toBe('function');
        expect(typeof window.UI.showConfirm).toBe('function');
    });

    test('showToast creates a toast notification', () => {
        window.UI.showToast('Test message', 'success');

        const toast = document.querySelector('.toast');
        expect(toast).toBeTruthy();
        expect(toast.textContent).toBe('Test message');
        expect(toast.classList.contains('toast-success')).toBe(true);
    });

    test('showModal creates a modal overlay', () => {
        window.UI.showModal('Test content');

        const overlay = document.querySelector('.modal-overlay');
        expect(overlay).toBeTruthy();

        const content = document.querySelector('.modal-content');
        expect(content.innerHTML).toBe('Test content');
    });

    test('hideModal removes the modal', () => {
        window.UI.showModal('Test content');
        expect(document.querySelector('.modal-overlay')).toBeTruthy();

        window.UI.hideModal();
        expect(document.querySelector('.modal-overlay')).toBeFalsy();
    });
});
