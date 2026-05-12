/**
 * Test helpers for loading HTML partials into jsdom.
 *
 * Created by the first scenario builder.
 *
 * Exports:
 *   loadPartial(relativePath) -> Document
 *     Loads a single HTML fragment into a fresh document body.
 *   loadFullPage() -> Document
 *     Returns a jsdom Document built from public/index.html with the
 *     section partials inlined (the index file already inlines them).
 *   withClipboardMock(fn) -> any
 *     Replaces navigator.clipboard with a writable mock for the duration
 *     of the synchronous fn call, then restores the original.
 */

const fs = require('fs');
const path = require('path');

const REPO_ROOT = path.resolve(__dirname, '..', '..');

function readFileFromRepo(relativePath) {
  const abs = path.join(REPO_ROOT, relativePath);
  return fs.readFileSync(abs, 'utf8');
}

function loadPartial(relativePath) {
  const html = readFileFromRepo(relativePath);
  document.documentElement.innerHTML = `<head></head><body>${html}</body>`;
  return document;
}

function loadFullPage() {
  const html = readFileFromRepo('public/index.html');
  const bodyMatch = html.match(/<body[^>]*>([\s\S]*)<\/body>/i);
  const bodyContent = bodyMatch ? bodyMatch[1] : html;
  document.documentElement.innerHTML = `<head></head><body>${bodyContent}</body>`;
  return document;
}

function withClipboardMock(fn) {
  const originalDescriptor = Object.getOwnPropertyDescriptor(
    global.navigator,
    'clipboard'
  );
  const mock = {
    writeText: jest.fn().mockResolvedValue(undefined),
  };
  Object.defineProperty(global.navigator, 'clipboard', {
    configurable: true,
    writable: true,
    value: mock,
  });
  try {
    return fn(mock);
  } finally {
    if (originalDescriptor) {
      Object.defineProperty(global.navigator, 'clipboard', originalDescriptor);
    } else {
      delete global.navigator.clipboard;
    }
  }
}

module.exports = { loadPartial, loadFullPage, withClipboardMock };
