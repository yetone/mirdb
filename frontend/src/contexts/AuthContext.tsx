import React, { createContext, useContext, useState, ReactNode } from 'react';

interface User {
  username: string;
  email: string;
  is_admin: boolean;
}

interface AuthContextValue {
  isAuthenticated: boolean;
  user: User | null;
  login: (user: User, token: string) => void;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

interface AuthProviderProps {
  children: ReactNode;
  initialAuth?: boolean;
  initialUser?: User | null;
}

export function AuthProvider({ children, initialAuth = false, initialUser = null }: AuthProviderProps) {
  const [isAuthenticated, setIsAuthenticated] = useState(initialAuth);
  const [user, setUser] = useState<User | null>(initialUser);

  const login = (userData: User, token: string) => {
    localStorage.setItem('token', token);
    setUser(userData);
    setIsAuthenticated(true);
  };

  const logout = () => {
    localStorage.removeItem('token');
    setUser(null);
    setIsAuthenticated(false);
  };

  return (
    <AuthContext.Provider value={{ isAuthenticated, user, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}

export { AuthContext };
