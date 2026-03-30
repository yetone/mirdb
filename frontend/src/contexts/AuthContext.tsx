/**
 * Authentication Context for managing user auth state
 */

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react'

interface User {
  id: number
  email: string
  username: string
  is_admin: boolean
}

interface AuthContextType {
  user: User | null
  isAuthenticated: boolean
  isLoading: boolean
  login: (email: string, password: string) => Promise<void>
  logout: () => void
  register: (email: string, username: string, password: string) => Promise<void>
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

interface AuthProviderProps {
  children: ReactNode
}

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const [user, setUser] = useState<User | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    // Check for existing token on mount
    const token = localStorage.getItem('token')
    if (token) {
      // In a real app, validate token and fetch user
      setIsLoading(false)
    } else {
      setIsLoading(false)
    }
  }, [])

  const login = async (email: string, password: string) => {
    // Placeholder implementation
    console.log('Login:', email, password)
  }

  const logout = () => {
    localStorage.removeItem('token')
    setUser(null)
  }

  const register = async (email: string, username: string, password: string) => {
    // Placeholder implementation
    console.log('Register:', email, username, password)
  }

  return (
    <AuthContext.Provider
      value={{
        user,
        isAuthenticated: !!user,
        isLoading,
        login,
        logout,
        register,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export const useAuth = (): AuthContextType => {
  const context = useContext(AuthContext)
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return context
}
