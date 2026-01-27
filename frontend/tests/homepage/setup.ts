/**
 * Homepage Test Setup
 *
 * Shared test utilities and configuration for homepage tests.
 */

export const mockAuthContext = {
  authenticated: {
    user: { id: 1, email: 'test@example.com', is_admin: false },
    isLoading: false,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
  },
  unauthenticated: {
    user: null,
    isLoading: false,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
  },
};

export const mockThemeContext = {
  light: {
    theme: 'light' as const,
    setTheme: vi.fn(),
  },
  dark: {
    theme: 'dark' as const,
    setTheme: vi.fn(),
  },
};
