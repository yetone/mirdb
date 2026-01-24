import React from 'react'
import { Link } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'
import ThemeToggle from './ThemeToggle'

export function Navbar() {
  const { isAuthenticated, logout } = useAuth()

  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-lg border-b border-base-300 sticky top-0 z-50">
      <div className="navbar-start">
        <Link to="/" className="btn btn-ghost text-xl font-bold">
          URL Shortener
        </Link>
      </div>
      <div className="navbar-end gap-2">
        <ThemeToggle />
        {isAuthenticated ? (
          <>
            <Link to="/dashboard" className="btn btn-ghost">
              Dashboard
            </Link>
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
              Register
            </Link>
          </>
        )}
      </div>
    </nav>
  )
}

export default Navbar
