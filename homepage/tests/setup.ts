import '@testing-library/jest-dom';

// Mock clipboard API
global.navigator.clipboard = {
  writeText: jest.fn(() => Promise.resolve()),
  readText: jest.fn(() => Promise.resolve('')),
} as unknown as Clipboard;

// Mock document.execCommand
Object.defineProperty(document, 'execCommand', {
  value: jest.fn(() => true),
  writable: true,
});
