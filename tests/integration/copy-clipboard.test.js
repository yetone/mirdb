/**
 * Copy-to-Clipboard Integration Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Tests:
 * - Copy button functionality with clipboard API
 * - Copy button fallback behavior when clipboard API unavailable
 * - Visual feedback (tooltip, icon change) after copy
 * - Button click event handling
 * - Error handling for missing copy target
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

const copyJsPath = path.join(__dirname, '../../js/copy.js');
const copyJs = fs.readFileSync(copyJsPath, 'utf-8');

describe('Copy Button - Clipboard API', () => {
  let mockClipboard;
  let writeTextSpy;

  beforeEach(() => {
    document.body.innerHTML = html;

    // Mock clipboard API
    writeTextSpy = jest.fn().mockResolvedValue(undefined);
    mockClipboard = {
      writeText: writeTextSpy,
    };
    Object.defineProperty(navigator, 'clipboard', {
      value: mockClipboard,
      writable: true,
      configurable: true,
    });
    Object.defineProperty(window, 'isSecureContext', {
      value: true,
      writable: true,
      configurable: true,
    });

    // Clear any prior state
    delete window.mirDB;

    // Execute copy.js
    eval(copyJs);
  });

  afterEach(() => {
    jest.clearAllMocks();
    jest.useRealTimers();
  });

  test('copy button calls clipboard.writeText on click', async () => {
    const button = document.querySelector('.copy-button');
    button.click();

    // Allow promises to resolve
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(writeTextSpy).toHaveBeenCalledTimes(1);
    const writtenText = writeTextSpy.mock.calls[0][0];
    expect(writtenText).toContain('set mykey');
    expect(writtenText).toContain('get mykey');
    expect(writtenText).toContain('delete mykey');
  });

  test('copy button shows success feedback after successful copy', async () => {
    const button = document.querySelector('.copy-button');
    button.click();

    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(button.classList.contains('copy-success')).toBe(true);
  });

  test('copy feedback is removed after timeout', async () => {
    const button = document.querySelector('.copy-button');

    // Spy on setTimeout to capture and control the callback
    const originalSetTimeout = window.setTimeout;
    const timeoutCallbacks = [];
    window.setTimeout = function(callback, delay) {
      timeoutCallbacks.push({ callback, delay });
      return 1;
    };

    button.click();

    // Wait for clipboard promise to resolve
    await new Promise((resolve) => originalSetTimeout(resolve, 10));

    // Class should be added after successful copy
    expect(button.classList.contains('copy-success')).toBe(true);

    // Execute the timeout callback that removes the class
    timeoutCallbacks.forEach((t) => t.callback());

    // Class should now be removed
    expect(button.classList.contains('copy-success')).toBe(false);

    // Restore original setTimeout
    window.setTimeout = originalSetTimeout;
  });

  test('copy button copies all code content including comments', async () => {
    const button = document.querySelector('.copy-button');
    button.click();

    await new Promise((resolve) => setTimeout(resolve, 10));

    const writtenText = writeTextSpy.mock.calls[0][0];
    expect(writtenText).toContain('# Connect to MirDB with telnet');
    expect(writtenText).toContain('STORED');
    expect(writtenText).toContain('DELETED');
  });
});

describe('Copy Button - Fallback Behavior', () => {
  let execCommandSpy;
  let getSelectionSpy;

  beforeEach(() => {
    document.body.innerHTML = html;

    // Remove clipboard API to force fallback
    Object.defineProperty(navigator, 'clipboard', {
      value: undefined,
      writable: true,
      configurable: true,
    });

    // Mock execCommand
    execCommandSpy = jest.fn().mockReturnValue(true);
    document.execCommand = execCommandSpy;

    // Mock window.getSelection
    const mockRange = {
      selectNodeContents: jest.fn(),
    };
    const mockSelection = {
      removeAllRanges: jest.fn(),
      addRange: jest.fn(),
    };
    getSelectionSpy = jest.fn().mockReturnValue(mockSelection);
    window.getSelection = getSelectionSpy;
    document.createRange = jest.fn().mockReturnValue(mockRange);

    delete window.mirDB;
    eval(copyJs);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  test('uses execCommand fallback when clipboard API is unavailable', async () => {
    const button = document.querySelector('.copy-button');
    button.click();

    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(execCommandSpy).toHaveBeenCalledWith('copy');
  });

  test('shows success feedback after fallback copy succeeds', async () => {
    const button = document.querySelector('.copy-button');
    button.click();

    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(button.classList.contains('copy-success')).toBe(true);
  });

  test('selects text when execCommand returns false', async () => {
    execCommandSpy.mockReturnValue(false);

    const button = document.querySelector('.copy-button');
    button.click();

    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(getSelectionSpy).toHaveBeenCalled();
  });

  test('does not throw when copy target is missing', async () => {
    const button = document.querySelector('.copy-button');
    button.setAttribute('data-copy-target', 'nonexistent');

    expect(() => {
      button.click();
    }).not.toThrow();
  });
});

describe('Copy Button - Clipboard API Error', () => {
  let writeTextSpy;
  let execCommandSpy;

  beforeEach(() => {
    document.body.innerHTML = html;

    // Mock clipboard API that rejects
    writeTextSpy = jest.fn().mockRejectedValue(new Error('Clipboard denied'));
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: writeTextSpy },
      writable: true,
      configurable: true,
    });
    Object.defineProperty(window, 'isSecureContext', {
      value: true,
      writable: true,
      configurable: true,
    });

    // Mock execCommand fallback
    execCommandSpy = jest.fn().mockReturnValue(true);
    document.execCommand = execCommandSpy;

    delete window.mirDB;
    eval(copyJs);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  test('falls back to execCommand when clipboard.writeText rejects', async () => {
    const button = document.querySelector('.copy-button');
    button.click();

    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(writeTextSpy).toHaveBeenCalledTimes(1);
    expect(execCommandSpy).toHaveBeenCalledWith('copy');
  });

  test('shows success feedback after execCommand fallback succeeds', async () => {
    const button = document.querySelector('.copy-button');
    button.click();

    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(button.classList.contains('copy-success')).toBe(true);
  });
});
