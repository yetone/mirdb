/**
 * Test utilities and custom render functions.
 * Owner: First Builder (Shared)
 */
import React, { ReactElement } from 'react';
import { render, RenderOptions, screen, fireEvent, waitFor } from '@testing-library/react';

// Custom render function with providers
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

// Export commonly used utilities
export { screen, fireEvent, waitFor };

// Mock theme utility
export const mockTheme = (theme: 'light' | 'dark') => {
  document.documentElement.setAttribute('data-theme', theme);
};
