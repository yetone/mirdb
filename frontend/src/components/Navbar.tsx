/**
 * Navigation Bar component.
 * Owner: Scenario 5 - Navigation Bar Functionality
 *
 * This component provides the main navigation for the homepage:
 * - Logo on left linking to homepage
 * - Menu items in center: Features, Pricing, About
 * - Authentication actions on right: Login, Get Started CTA
 * - Mobile: Collapsible hamburger menu with same options
 */

import React, { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { Menu, X, Link2 } from 'lucide-react'
import { useAuth } from '@/contexts/AuthContext'
import { useTheme } from '@/contexts/ThemeContext'

interface NavLinkProps {
  to: string
  children: React.ReactNode
  onClick?: () => void
  testId?: string
}

const NavLink: React.FC<NavLinkProps> = ({ to, children, onClick, testId }) => (
  <Link
    to={to}
    onClick={onClick}
    data-testid={testId}
    className="text-base-content hover:text-primary transition-colors duration-200 font-medium"
  >
    {children}
  </Link>
)

const Navbar: React.FC = () => {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)
  const { isAuthenticated } = useAuth()
  const { theme, toggleTheme } = useTheme()
  const navigate = useNavigate()

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen)
  }

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false)
  }

  const handleLogoClick = (e: React.MouseEvent) => {
    e.preventDefault()
    navigate('/')
    closeMobileMenu()
  }

  return (
    <nav
      data-testid="navbar"
      className="navbar bg-base-100 shadow-lg sticky top-0 z-50 px-4 lg:px-8"
      role="navigation"
      aria-label="Main navigation"
    >
      {/* Logo */}
      <div className="navbar-start">
        <a
          href="/"
          onClick={handleLogoClick}
          data-testid="navbar-logo"
          className="flex items-center gap-2 text-xl font-bold text-primary hover:opacity-80 transition-opacity"
          aria-label="Go to homepage"
        >
          <Link2 className="w-6 h-6" aria-hidden="true" />
          <span className="hidden sm:inline">LinkShort</span>
        </a>
      </div>

      {/* Desktop Menu - Center (visible at tablet and above) */}
      <div className="navbar-center hidden md:flex">
        <ul className="menu menu-horizontal gap-2">
          <li>
            <NavLink to="/features" testId="nav-features">
              Features
            </NavLink>
          </li>
          <li>
            <NavLink to="/pricing" testId="nav-pricing">
              Pricing
            </NavLink>
          </li>
          <li>
            <NavLink to="/about" testId="nav-about">
              About
            </NavLink>
          </li>
        </ul>
      </div>

      {/* Desktop Auth Links - Right (visible at tablet and above) */}
      <div className="navbar-end hidden md:flex items-center gap-4">
        {/* Theme Toggle */}
        <button
          onClick={toggleTheme}
          className="btn btn-ghost btn-circle"
          aria-label={`Switch to ${theme === 'light' ? 'dark' : 'light'} mode`}
          data-testid="theme-toggle"
        >
          {theme === 'light' ? (
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-5 w-5"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"
              />
            </svg>
          ) : (
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-5 w-5"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
              />
            </svg>
          )}
        </button>

        {!isAuthenticated && (
          <>
            <Link
              to="/login"
              data-testid="nav-login"
              className="btn btn-ghost"
            >
              Login
            </Link>
            <Link
              to="/register"
              data-testid="nav-get-started"
              className="btn btn-primary"
            >
              Get Started
            </Link>
          </>
        )}

        {isAuthenticated && (
          <Link
            to="/dashboard"
            data-testid="nav-dashboard"
            className="btn btn-primary"
          >
            Dashboard
          </Link>
        )}
      </div>

      {/* Mobile Menu Button (visible below tablet) */}
      <div className="navbar-end md:hidden">
        <button
          onClick={toggleMobileMenu}
          className="btn btn-ghost btn-square"
          aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
          aria-expanded={isMobileMenuOpen}
          data-testid="mobile-menu-toggle"
        >
          {isMobileMenuOpen ? (
            <X className="w-6 h-6" aria-hidden="true" />
          ) : (
            <Menu className="w-6 h-6" aria-hidden="true" />
          )}
        </button>
      </div>

      {/* Mobile Menu Dropdown */}
      {isMobileMenuOpen && (
        <div
          className="absolute top-full left-0 right-0 bg-base-100 shadow-lg md:hidden z-50"
          data-testid="mobile-menu"
        >
          <ul className="menu p-4">
            <li>
              <NavLink to="/features" onClick={closeMobileMenu} testId="mobile-nav-features">
                Features
              </NavLink>
            </li>
            <li>
              <NavLink to="/pricing" onClick={closeMobileMenu} testId="mobile-nav-pricing">
                Pricing
              </NavLink>
            </li>
            <li>
              <NavLink to="/about" onClick={closeMobileMenu} testId="mobile-nav-about">
                About
              </NavLink>
            </li>
            <li className="divider my-2" aria-hidden="true"></li>
            {!isAuthenticated && (
              <>
                <li>
                  <Link
                    to="/login"
                    onClick={closeMobileMenu}
                    data-testid="mobile-nav-login"
                    className="justify-center"
                  >
                    Login
                  </Link>
                </li>
                <li className="mt-2">
                  <Link
                    to="/register"
                    onClick={closeMobileMenu}
                    data-testid="mobile-nav-get-started"
                    className="btn btn-primary justify-center"
                  >
                    Get Started
                  </Link>
                </li>
              </>
            )}
            {isAuthenticated && (
              <li>
                <Link
                  to="/dashboard"
                  onClick={closeMobileMenu}
                  data-testid="mobile-nav-dashboard"
                  className="btn btn-primary justify-center"
                >
                  Dashboard
                </Link>
              </li>
            )}
          </ul>
        </div>
      )}
    </nav>
  )
}

export default Navbar
