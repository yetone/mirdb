import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  ReactNode,
} from 'react';

export interface AuthContextValue {
  isAuthenticated: boolean;
  loading: boolean;
  login: () => void;
  logout: () => void;
}

const defaultContextValue: AuthContextValue = {
  isAuthenticated: false,
  loading: false,
  login: () => {},
  logout: () => {},
};

const AuthContext = createContext<AuthContextValue>(defaultContextValue);

export function useAuth(): AuthContextValue {
  return useContext(AuthContext);
}

interface AuthProviderProps {
  children: ReactNode;
  initialAuthenticated?: boolean;
  initialLoading?: boolean;
}

export function AuthProvider({
  children,
  initialAuthenticated = false,
  initialLoading = false,
}: AuthProviderProps) {
  const [isAuthenticated, setIsAuthenticated] = useState<boolean>(
    initialAuthenticated,
  );
  const [loading] = useState<boolean>(initialLoading);

  const login = useCallback(() => {
    setIsAuthenticated(true);
  }, []);

  const logout = useCallback(() => {
    setIsAuthenticated(false);
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({ isAuthenticated, loading, login, logout }),
    [isAuthenticated, loading, login, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export default AuthContext;
