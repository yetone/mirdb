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
    <nav className="navbar bg-base-100 shadow-lg" role="navigation" aria-label="Main navigation">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl">
          URL Shortener
        </Link>
      </div>
      <div className="flex-none">
        {isAuthenticated ? (
          <div className="flex gap-2">
            <Link to="/dashboard" className="btn btn-ghost">
              Dashboard
            </Link>
            <button onClick={logout} className="btn btn-ghost">
              Logout
            </button>
          </div>
        ) : (
          <div className="flex gap-2">
            <Link to="/login" className="btn btn-ghost" data-testid="navbar-login">
              Log In
            </Link>
            <Link to="/register" className="btn btn-primary" data-testid="navbar-register">
              Sign Up
            </Link>
          </div>
        )}
      </div>
    </nav>
  )
}
