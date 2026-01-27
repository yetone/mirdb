/**
 * Test utilities and custom render.
 * Owner: First builder (shared)
 *
 * Provides custom render function that wraps components
 * with necessary providers:
 * - ThemeContext
 * - React Router (BrowserRouter/MemoryRouter)
 * - React Query (if needed)
 *
 * Expected exports:
 * - customRender: (ui, options?) => RenderResult
 * - All exports from @testing-library/react
 */

import { ReactElement, ReactNode } from 'react';
import { render, RenderOptions, RenderResult } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from '../src/contexts/ThemeContext';

interface WrapperProps {
  children: ReactNode;
}

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialEntries?: string[];
}

function AllProviders({ children }: WrapperProps) {
  return (
    <ThemeProvider>
      <MemoryRouter>
        {children}
      </MemoryRouter>
    </ThemeProvider>
  );
}

function createWrapper(initialEntries?: string[]) {
  return function Wrapper({ children }: WrapperProps) {
    return (
      <ThemeProvider>
        <MemoryRouter initialEntries={initialEntries || ['/']}>
          {children}
        </MemoryRouter>
      </ThemeProvider>
    );
  };
}

function customRender(
  ui: ReactElement,
  options?: CustomRenderOptions
): RenderResult {
  const { initialEntries, ...renderOptions } = options || {};
  const wrapper = initialEntries ? createWrapper(initialEntries) : AllProviders;
  return render(ui, { wrapper, ...renderOptions });
}

// Re-export everything
export * from '@testing-library/react';

// Override render with customRender
export { customRender as render };
