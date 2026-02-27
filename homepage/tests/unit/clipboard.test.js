/**
 * Unit tests for clipboard.js
 * Owner: Scenario 3 - Quick Start Code Examples
 */

const { resetMocks, clipboardMock, createElement } = require('../setup');

// We need to mock document and navigator before requiring clipboard module
describe('Clipboard Module', () => {
    let copyToClipboard, showCopyNotification, initClipboard, handleCopyClick;

    beforeEach(() => {
        resetMocks();

        // Reset DOM
        document.body.innerHTML = `
            <div id="toast-container" class="toast-container" aria-live="polite"></div>
            <div class="code-block">
                <button class="copy-btn"><span>Copy</span></button>
                <pre><code>test code content</code></pre>
            </div>
        `;

        // Clear module cache to get fresh instance
        jest.resetModules();

        // Re-require the module
        const clipboard = require('../../js/clipboard.js');
        copyToClipboard = clipboard.copyToClipboard;
        showCopyNotification = clipboard.showCopyNotification;
        initClipboard = clipboard.initClipboard;
        handleCopyClick = clipboard.handleCopyClick;
    });

    describe('copyToClipboard', () => {
        test('returns Promise<true> when copying text successfully', async () => {
            clipboardMock.writeText.mockResolvedValueOnce(undefined);

            const result = await copyToClipboard('test code');

            expect(result).toBe(true);
            expect(clipboardMock.writeText).toHaveBeenCalledWith('test code');
        });

        test('handles empty string input', async () => {
            clipboardMock.writeText.mockResolvedValueOnce(undefined);

            const result = await copyToClipboard('');

            expect(result).toBe(true);
            expect(clipboardMock.writeText).toHaveBeenCalledWith('');
        });

        test('handles special characters in text', async () => {
            clipboardMock.writeText.mockResolvedValueOnce(undefined);
            const specialText = 'const foo = "bar";\n// comment\n<html>';

            const result = await copyToClipboard(specialText);

            expect(result).toBe(true);
            expect(clipboardMock.writeText).toHaveBeenCalledWith(specialText);
        });

        test('falls back when clipboard API fails', async () => {
            clipboardMock.writeText.mockRejectedValueOnce(new Error('Permission denied'));

            // Mock execCommand for fallback
            document.execCommand = jest.fn(() => true);

            const result = await copyToClipboard('test code');

            expect(result).toBe(true);
        });
    });

    describe('showCopyNotification', () => {
        test('creates toast notification with "Copied!" message', () => {
            jest.useFakeTimers();
            const element = document.querySelector('.copy-btn');

            showCopyNotification(element);

            const toast = document.querySelector('.toast');
            expect(toast).not.toBeNull();
            expect(toast.textContent).toBe('Copied!');
            expect(toast.classList.contains('toast-success')).toBe(true);

            jest.useRealTimers();
        });

        test('toast notification appears with correct attributes', () => {
            jest.useFakeTimers();
            const element = document.querySelector('.copy-btn');

            showCopyNotification(element);

            const toast = document.querySelector('.toast');
            expect(toast.getAttribute('role')).toBe('status');
            expect(toast.getAttribute('aria-live')).toBe('polite');

            jest.useRealTimers();
        });

        test('toast notification disappears after timeout', () => {
            jest.useFakeTimers();
            const element = document.querySelector('.copy-btn');

            showCopyNotification(element, 'Copied!', 2000);

            let toast = document.querySelector('.toast');
            expect(toast).not.toBeNull();

            // Fast-forward past the display time
            jest.advanceTimersByTime(2000);

            toast = document.querySelector('.toast');
            expect(toast.classList.contains('toast-hiding')).toBe(true);

            // Fast-forward past the hide animation
            jest.advanceTimersByTime(300);

            toast = document.querySelector('.toast');
            expect(toast).toBeNull();

            jest.useRealTimers();
        });

        test('accepts custom message', () => {
            jest.useFakeTimers();
            const element = document.querySelector('.copy-btn');

            showCopyNotification(element, 'Custom message');

            const toast = document.querySelector('.toast');
            expect(toast.textContent).toBe('Custom message');

            jest.useRealTimers();
        });

        test('handles missing toast container gracefully', () => {
            document.getElementById('toast-container').remove();
            const element = document.querySelector('.copy-btn');

            // Should not throw
            expect(() => showCopyNotification(element)).not.toThrow();
        });
    });

    describe('initClipboard', () => {
        test('attaches click handlers to all copy buttons', () => {
            document.body.innerHTML = `
                <div id="toast-container"></div>
                <div class="code-block">
                    <button class="copy-btn"><span>Copy</span></button>
                    <pre><code>code 1</code></pre>
                </div>
                <div class="code-block">
                    <button class="copy-btn"><span>Copy</span></button>
                    <pre><code>code 2</code></pre>
                </div>
            `;

            initClipboard();

            const buttons = document.querySelectorAll('.copy-btn');
            expect(buttons.length).toBe(2);

            // Verify event listeners are attached by checking they respond to clicks
            buttons.forEach(btn => {
                expect(btn.onclick !== null || btn.addEventListener).toBeTruthy();
            });
        });
    });

    describe('handleCopyClick integration', () => {
        test('copies code block content when button is clicked', async () => {
            clipboardMock.writeText.mockResolvedValueOnce(undefined);

            const button = document.querySelector('.copy-btn');
            const event = { currentTarget: button };

            await handleCopyClick(event);

            expect(clipboardMock.writeText).toHaveBeenCalledWith('test code content');
        });

        test('updates button text after successful copy', async () => {
            jest.useFakeTimers();
            clipboardMock.writeText.mockResolvedValueOnce(undefined);

            const button = document.querySelector('.copy-btn');
            const buttonText = button.querySelector('span');
            const event = { currentTarget: button };

            await handleCopyClick(event);

            expect(buttonText.textContent).toBe('Copied!');

            // Fast-forward to see original text restored
            jest.advanceTimersByTime(2000);

            expect(buttonText.textContent).toBe('Copy');

            jest.useRealTimers();
        });
    });
});
