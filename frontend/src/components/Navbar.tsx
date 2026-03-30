/**
 * Navigation bar component
 */

import React from 'react'
import { Link } from 'react-router-dom'
import { useAuth } from '@/contexts/AuthContext'
import ThemeToggle from './ThemeToggle'

const Navbar: React.FC = () => {
  const { isAuthenticated, user, logout } = useAuth()

  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-md fixed top-0 z-50 shadow-lg">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl">
          URL Shortener
        </Link>
      </div>
      <div className="flex-none gap-2">
        <ThemeToggle />
        {isAuthenticated ? (
          <>
            <span className="text-sm">Welcome, {user?.username}</span>
            <Link to="/dashboard" className="btn btn-ghost">
              Dashboard
            </Link>
            {user?.is_admin && (
              <Link to="/settings" className="btn btn-ghost">
                Settings
              </Link>
            )}
            <button onClick={logout} className="btn btn-ghost">
              Logout
            </button>
          </>
        ) : (
          <>
            <Link to="/login" className="btn btn-ghost">
              Login
            </Link>
            <Link to="/register" className="btn btn-primary">
              Sign Up
            </Link>
          </>
        )}
      </div>
    </nav>
  )
}

export default Navbar
