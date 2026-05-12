/**
 * Tests for clipboard.js + toast helper interaction (Scenario 3).
 * Covers test cases 5, 6, and 7 from .something/scenario.json.
 */

const { loadFullPage } = require('../helpers/dom');

function flushMicrotasks() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function setClipboard(value) {
  const desc = Object.getOwnPropertyDescriptor(global.navigator, 'clipboard');
  Object.defineProperty(global.navigator, 'clipboard', {
    configurable: true,
    writable: true,
    value,
  });
  return desc;
}

describe('clipboard.js: copy button behavior', () => {
  let originalClipboardDescriptor;

  beforeEach(() => {
    jest.resetModules();
    originalClipboardDescriptor = Object.getOwnPropertyDescriptor(
      global.navigator,
      'clipboard'
    );
  });

  afterEach(() => {
    if (originalClipboardDescriptor) {
      Object.defineProperty(
        global.navigator,
        'clipboard',
        originalClipboardDescriptor
      );
    } else {
      delete global.navigator.clipboard;
    }
  });

  test('test case 5: click calls writeText once with the code element textContent', async () => {
    const doc = loadFullPage();
    const writeText = jest.fn().mockResolvedValue(undefined);
    setClipboard({ writeText });

    const clipboardModule = require('../../src/scripts/clipboard.js');
    clipboardModule.initClipboard(doc);

    const code = doc.querySelector('#quick-start pre > code');
    const expectedText = code.textContent;

    const button = doc.querySelector('#quick-start .copy-btn');
    button.click();
    await flushMicrotasks();

    expect(writeText).toHaveBeenCalledTimes(1);
    expect(writeText).toHaveBeenCalledWith(expectedText);
  });

  test('test case 6: success path shows a success toast with a non-empty message', async () => {
    const doc = loadFullPage();
    const writeText = jest.fn().mockResolvedValue(undefined);
    setClipboard({ writeText });

    const clipboardModule = require('../../src/scripts/clipboard.js');
    const showToast = jest.fn();

    const ok = await clipboardModule.copyText('hello world', {
      toast: showToast,
    });

    expect(ok).toBe(true);
    expect(showToast).toHaveBeenCalledTimes(1);
    const [message, opts] = showToast.mock.calls[0];
    expect(typeof message).toBe('string');
    expect(message.length).toBeGreaterThan(0);
    expect(opts).toBeDefined();
    expect(opts.variant).toBe('success');
  });

  test('test case 6 (DOM): integration emits an actual success toast into the DOM', async () => {
    const doc = loadFullPage();
    const writeText = jest.fn().mockResolvedValue(undefined);
    setClipboard({ writeText });

    const clipboardModule = require('../../src/scripts/clipboard.js');
    clipboardModule.initClipboard(doc);

    const button = doc.querySelector('#quick-start .copy-btn');
    button.click();
    await flushMicrotasks();

    const region = doc.getElementById('toast-region');
    expect(region).not.toBeNull();
    const toast = region.querySelector('.toast--success');
    expect(toast).not.toBeNull();
    expect((toast.textContent || '').length).toBeGreaterThan(0);
    expect(toast.getAttribute('data-variant')).toBe('success');
  });

  test('test case 7: rejection path -> copyText resolves false and emits an error toast', async () => {
    const doc = loadFullPage();
    const error = new Error('Clipboard access denied');
    const writeText = jest.fn().mockRejectedValue(error);
    setClipboard({ writeText });

    const clipboardModule = require('../../src/scripts/clipboard.js');
    const showToast = jest.fn();

    const ok = await clipboardModule.copyText('payload', { toast: showToast });

    expect(ok).toBe(false);
    expect(showToast).toHaveBeenCalledTimes(1);
    const [message, opts] = showToast.mock.calls[0];
    expect(typeof message).toBe('string');
    expect(message.length).toBeGreaterThan(0);
    expect(opts).toBeDefined();
    expect(opts.variant).toBe('error');
    expect(writeText).toHaveBeenCalledTimes(1);
  });

  test('test case 7 (DOM): click while rejection happens still emits an error toast in DOM', async () => {
    const doc = loadFullPage();
    const writeText = jest.fn().mockRejectedValue(new Error('denied'));
    setClipboard({ writeText });

    const clipboardModule = require('../../src/scripts/clipboard.js');
    clipboardModule.initClipboard(doc);

    const button = doc.querySelector('#quick-start .copy-btn');
    button.click();
    await flushMicrotasks();
    await flushMicrotasks();

    const region = doc.getElementById('toast-region');
    expect(region).not.toBeNull();
    const toast = region.querySelector('.toast--error');
    expect(toast).not.toBeNull();
    expect(toast.getAttribute('data-variant')).toBe('error');
  });

  test('clipboard module exports expected API', () => {
    const clipboardModule = require('../../src/scripts/clipboard.js');
    expect(typeof clipboardModule.initClipboard).toBe('function');
    expect(typeof clipboardModule.copyText).toBe('function');
  });

  test('toast module exports showToast', () => {
    const toastModule = require('../../src/scripts/toast.js');
    expect(typeof toastModule.showToast).toBe('function');
  });
});
