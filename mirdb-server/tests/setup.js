/**
 * Jest Test Setup
 * Configures jsdom environment for frontend testing
 */

// Mock clipboard API
Object.assign(navigator, {
    clipboard: {
        writeText: jest.fn().mockResolvedValue(undefined),
        readText: jest.fn().mockResolvedValue('')
    }
});

// Mock scrollIntoView
Element.prototype.scrollIntoView = jest.fn();

// Mock window.scrollTo
window.scrollTo = jest.fn();

// Reset mocks before each test
beforeEach(() => {
    jest.clearAllMocks();
    document.body.innerHTML = '';
});
