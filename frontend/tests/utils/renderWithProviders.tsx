/**
 * Test utility for rendering components with required context providers.
 * Owner: First scenario builder
 *
 * Expected exports:
 * - renderWithProviders(ui, options): RenderResult
 * - AllTheProviders: React component wrapper
 *
 * Wraps components with:
 * - BrowserRouter for React Router
 * - ThemeProvider for theme context
 * - AuthProvider for authentication context (optional)
 */

import React, { ReactElement, ReactNode } from 'react';
import { render, RenderOptions, RenderResult } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from '@/contexts/ThemeContext';
import { AuthProvider } from '@/contexts/AuthContext';

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialEntries?: string[];
  withAuth?: boolean;
}

export function AllTheProviders({ children }: { children: ReactNode }) {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>
          {children}
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  );
}

export function renderWithProviders(
  ui: ReactElement,
  options: CustomRenderOptions = {}
): RenderResult {
  const { initialEntries, withAuth = true, ...renderOptions } = options;

  const Wrapper = ({ children }: { children: ReactNode }) => {
    const Router = initialEntries
      ? ({ children }: { children: ReactNode }) => (
          <MemoryRouter initialEntries={initialEntries}>{children}</MemoryRouter>
        )
      : BrowserRouter;

    return (
      <Router>
        <ThemeProvider>
          {withAuth ? (
            <AuthProvider>{children}</AuthProvider>
          ) : (
            children
          )}
        </ThemeProvider>
      </Router>
    );
  };

  return render(ui, { wrapper: Wrapper, ...renderOptions });
}
