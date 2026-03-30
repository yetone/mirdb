/**
 * Navigation bar component with mobile hamburger menu
 */

import React, { useState } from 'react'
import { Link } from 'react-router-dom'
import { Menu, X } from 'lucide-react'
import { useAuth } from '@/contexts/AuthContext'
import ThemeToggle from './ThemeToggle'

const Navbar: React.FC = () => {
  const { isAuthenticated, user, logout } = useAuth()
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen)
  }

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false)
  }

  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-md fixed top-0 z-50 shadow-lg" data-testid="navbar">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl min-h-[44px] min-w-[44px]">
          URL Shortener
        </Link>
      </div>

      {/* Desktop Navigation */}
      <div className="hidden md:flex flex-none gap-2" data-testid="desktop-nav">
        <ThemeToggle />
        {isAuthenticated ? (
          <>
            <span className="text-sm flex items-center">Welcome, {user?.username}</span>
            <Link to="/dashboard" className="btn btn-ghost min-h-[44px] min-w-[44px]">
              Dashboard
            </Link>
            {user?.is_admin && (
              <Link to="/settings" className="btn btn-ghost min-h-[44px] min-w-[44px]">
                Settings
              </Link>
            )}
            <button onClick={logout} className="btn btn-ghost min-h-[44px] min-w-[44px]">
              Logout
            </button>
          </>
        ) : (
          <>
            <Link to="/login" className="btn btn-ghost min-h-[44px] min-w-[44px]" data-testid="desktop-login">
              Login
            </Link>
            <Link to="/register" className="btn btn-primary min-h-[44px] min-w-[44px]" data-testid="desktop-register">
              Sign Up
            </Link>
          </>
        )}
      </div>

      {/* Mobile Navigation */}
      <div className="flex md:hidden gap-2" data-testid="mobile-nav-controls">
        <ThemeToggle />
        <button
          onClick={toggleMobileMenu}
          className="btn btn-ghost w-11 h-11 min-w-11 min-h-11 p-2"
          aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
          aria-expanded={isMobileMenuOpen}
          data-testid="hamburger-menu"
        >
          {isMobileMenuOpen ? (
            <X className="w-6 h-6" />
          ) : (
            <Menu className="w-6 h-6" />
          )}
        </button>
      </div>

      {/* Mobile Menu Dropdown */}
      {isMobileMenuOpen && (
        <div
          className="absolute top-full left-0 right-0 bg-base-100/95 backdrop-blur-md shadow-lg md:hidden"
          data-testid="mobile-menu"
        >
          <div className="flex flex-col p-4 gap-2">
            {isAuthenticated ? (
              <>
                <span className="text-sm px-4 py-2">Welcome, {user?.username}</span>
                <Link
                  to="/dashboard"
                  className="btn btn-ghost justify-start min-h-[44px]"
                  onClick={closeMobileMenu}
                >
                  Dashboard
                </Link>
                <button
                  onClick={() => {
                    logout()
                    closeMobileMenu()
                  }}
                  className="btn btn-ghost justify-start min-h-[44px]"
                >
                  Logout
                </button>
              </>
            ) : (
              <>
                <Link
                  to="/login"
                  className="btn btn-ghost justify-start min-h-[44px]"
                  onClick={closeMobileMenu}
                  data-testid="mobile-login"
                >
                  Login
                </Link>
                <Link
                  to="/register"
                  className="btn btn-primary justify-start min-h-[44px]"
                  onClick={closeMobileMenu}
                  data-testid="mobile-register"
                >
                  Register
                </Link>
              </>
            )}
          </div>
        </div>
      )}
    </nav>
  )
}

export default Navbar
