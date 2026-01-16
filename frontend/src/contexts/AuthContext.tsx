import { createContext, useContext, useState, useEffect, ReactNode } from 'react'

export interface User {
  id: string
  username: string
  email: string
  is_admin?: boolean
}

interface AuthContextType {
  user: User | null
  loading: boolean
  isAuthenticated: boolean
  isAdmin: boolean
  login: (username: string, password: string) => Promise<void>
  logout: () => void
  setUser: (user: User | null) => void
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

interface AuthProviderProps {
  children: ReactNode
  initialUser?: User | null
}

export function AuthProvider({ children, initialUser = null }: AuthProviderProps) {
  const [user, setUser] = useState<User | null>(initialUser)
  const [loading, setLoading] = useState(!initialUser)

  const isAuthenticated = user !== null
  const isAdmin = user?.is_admin ?? false

  useEffect(() => {
    // Skip if we have an initial user (useful for testing)
    if (initialUser !== null) {
      setLoading(false)
      return
    }

    const checkAuth = async () => {
      const token = localStorage.getItem('token')
      if (token) {
        try {
          // In a real app, this would validate the token with the backend
          const storedUser = localStorage.getItem('user')
          if (storedUser) {
            setUser(JSON.parse(storedUser))
          }
        } catch {
          localStorage.removeItem('token')
          localStorage.removeItem('user')
        }
      }
      setLoading(false)
    }
    checkAuth()
  }, [initialUser])

  const login = async (username: string, password: string) => {
    // In a real app, this would call the login API
    // For now, we simulate a login
    const mockUser: User = {
      id: '1',
      username,
      email: `${username}@example.com`,
      is_admin: false,
    }
    localStorage.setItem('token', 'mock-token')
    localStorage.setItem('user', JSON.stringify(mockUser))
    setUser(mockUser)
  }

  const logout = () => {
    localStorage.removeItem('token')
    localStorage.removeItem('user')
    setUser(null)
  }

  return (
    <AuthContext.Provider
      value={{
        user,
        loading,
        isAuthenticated,
        isAdmin,
        login,
        logout,
        setUser,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth(): AuthContextType {
  const context = useContext(AuthContext)
  if (context === undefined) {
    // Return a safe default for components used outside the provider
    return {
      user: null,
      loading: false,
      isAuthenticated: false,
      isAdmin: false,
      login: async () => {},
      logout: () => {},
      setUser: () => {},
    }
  }
  return context
}

export { AuthContext }
