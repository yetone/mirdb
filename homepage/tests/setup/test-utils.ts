/**
 * Test utilities and custom render functions.
 * Owner: First Builder (Shared)
 */
import '@testing-library/jest-dom';
import { render, RenderOptions } from '@testing-library/react';
import React, { ReactElement } from 'react';

// Custom render function with providers if needed
function customRender(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return render(ui, { ...options });
}

// Re-export everything from testing-library
export * from '@testing-library/react';

// Override render with custom render
export { customRender as render };

// Mock theme context
export const mockTheme = {
  theme: 'light' as const,
  setTheme: jest.fn(),
  toggleTheme: jest.fn(),
};
