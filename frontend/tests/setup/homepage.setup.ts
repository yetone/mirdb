/**
 * Test setup utilities for homepage tests.
 *
 * This file provides common test utilities and mocks for homepage component testing.
 */

import { ReactElement } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from 'react-query';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { AuthProvider } from '../../src/contexts/AuthContext';
import { vi } from 'vitest';

// Create a wrapper with all providers
interface WrapperProps {
  children: React.ReactNode;
}

const createTestQueryClient = () => new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
    },
  },
});

export const AllProviders = ({ children }: WrapperProps) => {
  const testQueryClient = createTestQueryClient();

  return (
    <QueryClientProvider client={testQueryClient}>
      <ThemeProvider>
        <AuthProvider>
          <BrowserRouter>
            {children}
          </BrowserRouter>
        </AuthProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
};

// Custom render function with providers
export const renderWithProviders = (
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) => render(ui, { wrapper: AllProviders, ...options });

// Mock authentication context
export const mockAuth = {
  user: null,
  loading: false,
  isAuthenticated: false,
  isAdmin: false,
  login: vi.fn(),
  logout: vi.fn(),
};

export const mockAuthenticatedUser = {
  user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
  loading: false,
  isAuthenticated: true,
  isAdmin: false,
  login: vi.fn(),
  logout: vi.fn(),
};

export const mockAdminUser = {
  user: { id: 1, username: 'admin', email: 'admin@example.com', is_admin: 1 },
  loading: false,
  isAuthenticated: true,
  isAdmin: true,
  login: vi.fn(),
  logout: vi.fn(),
};

// Re-export testing library utilities
export * from '@testing-library/react';
export { default as userEvent } from '@testing-library/user-event';
