/**
 * Authentication Context.
 *
 * Provides authentication state management throughout the application:
 * - User authentication status
 * - User information (when authenticated)
 * - Login/logout functionality
 * - JWT token management with expiration handling
 *
 * Used by: HomeNavbar (auth-aware navigation), ProtectedLayout
 */
import { createContext, useContext, useState, useEffect, useCallback, ReactNode } from 'react';

export interface User {
  id: string;
  email: string;
  name?: string;
}

export interface AuthContextType {
  isAuthenticated: boolean;
  isLoading: boolean;
  user: User | null;
  token: string | null;
  login: (token: string, user: User) => void;
  logout: () => void;
  isTokenExpired: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

const TOKEN_KEY = 'auth_token';
const USER_KEY = 'auth_user';

/**
 * Checks if a JWT token is expired
 * @param token - The JWT token string
 * @returns true if expired, false otherwise
 */
function checkTokenExpired(token: string | null): boolean {
  if (!token) return false;

  try {
    // JWT tokens are in format: header.payload.signature
    const parts = token.split('.');
    if (parts.length !== 3) return true;

    // Decode the payload (base64url encoded)
    const payload = JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')));

    // Check if exp claim exists and if it's expired
    if (payload.exp) {
      const expirationTime = payload.exp * 1000; // Convert to milliseconds
      return Date.now() >= expirationTime;
    }

    return false;
  } catch {
    // If we can't parse the token, consider it expired
    return true;
  }
}

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [isLoading, setIsLoading] = useState(true);
  const [user, setUser] = useState<User | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [isTokenExpired, setIsTokenExpired] = useState(false);

  // Initialize auth state from localStorage
  useEffect(() => {
    const storedToken = localStorage.getItem(TOKEN_KEY);
    const storedUser = localStorage.getItem(USER_KEY);

    if (storedToken && storedUser) {
      const expired = checkTokenExpired(storedToken);
      setIsTokenExpired(expired);

      if (!expired) {
        setToken(storedToken);
        try {
          setUser(JSON.parse(storedUser));
        } catch {
          // Invalid stored user, clear everything
          localStorage.removeItem(TOKEN_KEY);
          localStorage.removeItem(USER_KEY);
        }
      } else {
        // Token is expired - keep in state but mark as expired
        // This allows UI to show appropriate message
        setToken(storedToken);
        try {
          setUser(JSON.parse(storedUser));
        } catch {
          localStorage.removeItem(TOKEN_KEY);
          localStorage.removeItem(USER_KEY);
        }
      }
    }

    setIsLoading(false);
  }, []);

  // Check for token expiration periodically
  useEffect(() => {
    if (!token) return;

    const checkExpiration = () => {
      const expired = checkTokenExpired(token);
      setIsTokenExpired(expired);
    };

    // Check immediately
    checkExpiration();

    // Check every minute
    const interval = setInterval(checkExpiration, 60000);

    return () => clearInterval(interval);
  }, [token]);

  const login = useCallback((newToken: string, newUser: User) => {
    setToken(newToken);
    setUser(newUser);
    setIsTokenExpired(checkTokenExpired(newToken));
    localStorage.setItem(TOKEN_KEY, newToken);
    localStorage.setItem(USER_KEY, JSON.stringify(newUser));
  }, []);

  const logout = useCallback(() => {
    setToken(null);
    setUser(null);
    setIsTokenExpired(false);
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
  }, []);

  const value: AuthContextType = {
    isAuthenticated: !!token && !isTokenExpired,
    isLoading,
    user,
    token,
    login,
    logout,
    isTokenExpired,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

/**
 * Hook to access authentication context
 * @throws Error if used outside of AuthProvider
 */
export function useAuth(): AuthContextType {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
