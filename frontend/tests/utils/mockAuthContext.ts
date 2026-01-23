/**
 * Mock AuthContext for testing auth states
 *
 * Owner: First Builder (Shared Resource)
 */

import { vi } from 'vitest';

interface User {
  id: number;
  username: string;
  email: string;
  is_admin: number;
}

interface AuthContextType {
  user: User | null;
  loading: boolean;
  isAuthenticated: boolean;
  isAdmin: boolean;
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
}

export const mockUnauthenticatedContext: AuthContextType = {
  user: null,
  loading: false,
  isAuthenticated: false,
  isAdmin: false,
  login: vi.fn(),
  logout: vi.fn(),
};

export const mockAuthenticatedContext: AuthContextType = {
  user: {
    id: 1,
    username: 'testuser',
    email: 'test@example.com',
    is_admin: 0,
  },
  loading: false,
  isAuthenticated: true,
  isAdmin: false,
  login: vi.fn(),
  logout: vi.fn(),
};

export const mockAdminContext: AuthContextType = {
  user: {
    id: 1,
    username: 'admin',
    email: 'admin@example.com',
    is_admin: 1,
  },
  loading: false,
  isAuthenticated: true,
  isAdmin: true,
  login: vi.fn(),
  logout: vi.fn(),
};

export const mockLoadingContext: AuthContextType = {
  user: null,
  loading: true,
  isAuthenticated: false,
  isAdmin: false,
  login: vi.fn(),
  logout: vi.fn(),
};
