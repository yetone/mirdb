import '@testing-library/jest-dom';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import React from 'react';

// Mock matchMedia for JSDOM
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: jest.fn().mockImplementation(query => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: jest.fn(),
    removeListener: jest.fn(),
    addEventListener: jest.fn(),
    removeEventListener: jest.fn(),
    dispatchEvent: jest.fn(),
  })),
});

// Re-export everything from testing-library
export { render, screen, fireEvent, waitFor };

// Custom render with providers if needed
export function customRender(ui: React.ReactElement, options = {}) {
  return render(ui, { ...options });
}

// Mock theme utilities
export function mockTheme(theme: 'light' | 'dark') {
  document.documentElement.setAttribute('data-theme', theme);
}
