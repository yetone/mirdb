/**
 * Shared test utilities for homepage integration tests.
 *
 * Provides:
 * - renderWithRouter: Wrapper with React Router
 * - renderWithAllProviders: Full provider tree wrapper
 * - mockNavigate: Jest/Vitest mock for navigation
 * - Viewport helpers for responsive testing
 */

import React, { ReactElement } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter, MemoryRouter, MemoryRouterProps } from 'react-router-dom';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';

interface ProvidersProps {
  children: React.ReactNode;
}

/**
 * All providers wrapper for full integration tests
 */
function AllProviders({ children }: ProvidersProps) {
  return (
    <ThemeProvider>
      <AuthProvider>
        <BrowserRouter>{children}</BrowserRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}

/**
 * Render with all providers (AuthContext, ThemeContext, BrowserRouter)
 */
export function renderWithAllProviders(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return render(ui, { wrapper: AllProviders, ...options });
}

interface MemoryRouterProvidersProps {
  children: React.ReactNode;
  initialEntries?: MemoryRouterProps['initialEntries'];
}

/**
 * Create providers with MemoryRouter for controlled routing tests
 */
function createMemoryRouterProviders(initialEntries: MemoryRouterProps['initialEntries'] = ['/']) {
  return function MemoryRouterProviders({ children }: { children: React.ReactNode }) {
    return (
      <ThemeProvider>
        <AuthProvider>
          <MemoryRouter initialEntries={initialEntries}>{children}</MemoryRouter>
        </AuthProvider>
      </ThemeProvider>
    );
  };
}

/**
 * Render with MemoryRouter for navigation testing
 */
export function renderWithRouter(
  ui: ReactElement,
  { initialEntries = ['/'], ...options }: RenderOptions & { initialEntries?: string[] } = {}
) {
  const Wrapper = createMemoryRouterProviders(initialEntries);
  return render(ui, { wrapper: Wrapper, ...options });
}

/**
 * Mock authentication context values for testing different auth states
 */
export const mockAuthContext = {
  authenticated: {
    user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: false },
    isAuthenticated: true,
    isLoading: false,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
  },
  unauthenticated: {
    user: null,
    isAuthenticated: false,
    isLoading: false,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
  },
  loading: {
    user: null,
    isAuthenticated: false,
    isLoading: true,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
  },
};

/**
 * Viewport configurations for responsive testing
 */
export const viewports = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
  largeDesktop: { width: 1920, height: 1080 },
};

/**
 * Set viewport size for testing
 * Note: This requires additional setup in jsdom environment
 */
export function setViewport(viewport: keyof typeof viewports) {
  const { width, height } = viewports[viewport];
  Object.defineProperty(window, 'innerWidth', { writable: true, configurable: true, value: width });
  Object.defineProperty(window, 'innerHeight', { writable: true, configurable: true, value: height });
  window.dispatchEvent(new Event('resize'));
}
