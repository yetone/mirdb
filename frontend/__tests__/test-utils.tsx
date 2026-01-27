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

import React, { ReactElement } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from '../src/contexts/ThemeContext';

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialRoute?: string;
  useMemoryRouter?: boolean;
}

const AllProviders: React.FC<{ children: React.ReactNode; initialRoute?: string; useMemoryRouter?: boolean }> = ({
  children,
  initialRoute = '/',
  useMemoryRouter = false,
}) => {
  const Router = useMemoryRouter ? MemoryRouter : BrowserRouter;
  const routerProps = useMemoryRouter ? { initialEntries: [initialRoute] } : {};

  return (
    <ThemeProvider>
      <Router {...routerProps}>
        {children}
      </Router>
    </ThemeProvider>
  );
};

const customRender = (
  ui: ReactElement,
  options?: CustomRenderOptions
) => {
  const { initialRoute, useMemoryRouter, ...renderOptions } = options || {};

  return render(ui, {
    wrapper: ({ children }) => (
      <AllProviders initialRoute={initialRoute} useMemoryRouter={useMemoryRouter}>
        {children}
      </AllProviders>
    ),
    ...renderOptions,
  });
};

// Re-export everything
export * from '@testing-library/react';
export { customRender as render };
