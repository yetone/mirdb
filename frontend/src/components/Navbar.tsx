/**
 * Navbar Component
 *
 * Navigation header with links to login and registration.
 * Shows different options based on authentication state.
 */

import React from 'react'
import { Link } from 'react-router-dom'
import { useAuth } from '@/contexts/AuthContext'

export function Navbar() {
  const { isAuthenticated, logout } = useAuth()

  return (
    <nav className="navbar bg-base-100 shadow-lg px-2 sm:px-4" role="navigation" aria-label="Main navigation">
      <div className="flex-1 min-w-0">
        <Link to="/" className="btn btn-ghost text-base sm:text-xl truncate">
          URL Shortener
        </Link>
      </div>
      <div className="flex-none">
        {isAuthenticated ? (
          <div className="flex gap-1 sm:gap-2">
            <Link to="/dashboard" className="btn btn-ghost btn-sm sm:btn-md">
              Dashboard
            </Link>
            <button onClick={logout} className="btn btn-ghost btn-sm sm:btn-md">
              Logout
            </button>
          </div>
        ) : (
          <div className="flex gap-1 sm:gap-2">
            <Link to="/login" className="btn btn-ghost btn-sm sm:btn-md" data-testid="navbar-login">
              Log In
            </Link>
            <Link to="/register" className="btn btn-primary btn-sm sm:btn-md" data-testid="navbar-register">
              Sign Up
            </Link>
          </div>
        )}
      </div>
    </nav>
  )
}
