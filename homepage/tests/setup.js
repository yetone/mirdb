/**
 * Test Setup Utilities
 * Shared by: All test scenarios
 *
 * Purpose:
 * - Setup JSDOM environment
 * - Load common test utilities
 * - Configure global test helpers
 */

// Helper to set viewport size for responsive tests
export function setViewportWidth(width) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.dispatchEvent(new Event('resize'));
}

// Helper to load HTML content into the DOM
export function loadHTML(html) {
  document.body.innerHTML = html;
}

// Helper to wait for DOM updates
export function nextTick() {
  return new Promise(resolve => setTimeout(resolve, 0));
}

// Helper to simulate click events
export function click(element) {
  element.dispatchEvent(new MouseEvent('click', {
    bubbles: true,
    cancelable: true,
    view: window,
  }));
}

// Helper to simulate keyboard events
export function pressKey(element, key) {
  element.dispatchEvent(new KeyboardEvent('keydown', {
    key,
    bubbles: true,
    cancelable: true,
  }));
}
