import { createContext, useContext, useState, ReactNode } from 'react'

interface User {
  id: number
  email: string
  is_admin: boolean
}

interface AuthContextType {
  user: User | null
  isAuthenticated: boolean
  login: (email: string, password: string) => Promise<void>
  logout: () => void
  register: (email: string, password: string) => Promise<void>
}

const AuthContext = createContext<AuthContextType | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)

  const login = async (email: string, _password: string) => {
    // Mock login
    setUser({ id: 1, email, is_admin: false })
  }

  const logout = () => {
    setUser(null)
  }

  const register = async (email: string, _password: string) => {
    // Mock register
    setUser({ id: 1, email, is_admin: false })
  }

  return (
    <AuthContext.Provider
      value={{
        user,
        isAuthenticated: !!user,
        login,
        logout,
        register,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return context
}
