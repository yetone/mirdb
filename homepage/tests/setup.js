/**
 * Test Setup and Configuration
 *
 * Common utilities and setup for all test types.
 *
 * Exports:
 * - Test environment configuration
 * - Mock utilities for localStorage, clipboard API
 * - Helper functions for DOM testing
 */

// Mock localStorage
const localStorageMock = (() => {
    let store = {};
    return {
        getItem: jest.fn((key) => store[key] || null),
        setItem: jest.fn((key, value) => {
            store[key] = value.toString();
        }),
        removeItem: jest.fn((key) => {
            delete store[key];
        }),
        clear: jest.fn(() => {
            store = {};
        }),
    };
})();

// Mock clipboard API
const clipboardMock = {
    writeText: jest.fn(() => Promise.resolve()),
    readText: jest.fn(() => Promise.resolve('')),
};

// Set up global mocks
Object.defineProperty(window, 'localStorage', {
    value: localStorageMock,
});

Object.defineProperty(navigator, 'clipboard', {
    value: clipboardMock,
    writable: true,
});

// Helper to reset mocks between tests
function resetMocks() {
    localStorageMock.clear();
    jest.clearAllMocks();
}

// Helper to create a DOM element
function createElement(html) {
    const template = document.createElement('template');
    template.innerHTML = html.trim();
    return template.content.firstChild;
}

module.exports = {
    localStorageMock,
    clipboardMock,
    resetMocks,
    createElement,
};
