/**
 * Navigation Bar Component
 * Owner: Scenario 7 - Navigation Bar
 * Modified by: Scenario 9 - Responsive Design - Mobile (hamburger menu)
 *
 * Displays the navigation bar with:
 * - Logo/brand link to homepage
 * - Login and Register links (for unauthenticated users)
 * - Dashboard link (for authenticated users only)
 * - Theme toggle
 * - Mobile hamburger menu for responsive design
 *
 * Uses AuthContext to determine which links to display based on authentication state.
 */

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useAuth } from '@/contexts/AuthContext'
import { useTheme } from '@/contexts/ThemeContext'

export function Navbar() {
  const { isAuthenticated } = useAuth()
  const { theme, setTheme, themes } = useTheme()
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

  const handleThemeChange = (newTheme: string) => {
    setTheme(newTheme)
  }

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen)
  }

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false)
  }

  return (
    <nav className="navbar bg-base-200 px-4 shadow-lg" aria-label="Main navigation">
      <div className="navbar-start">
        <Link
          to="/"
          className="btn btn-ghost text-xl normal-case"
          aria-label="URL Shortener - Go to homepage"
        >
          URL Shortener
        </Link>
      </div>

      {/* Desktop navigation - hidden on mobile */}
      <div className="navbar-end gap-2 hidden md:flex">
        {/* Theme selector dropdown */}
        <div className="dropdown dropdown-end">
          <div tabIndex={0} role="button" className="btn btn-ghost min-h-[44px] min-w-[44px]">
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
                d="M7 21a4 4 0 01-4-4V5a2 2 0 012-2h4a2 2 0 012 2v12a4 4 0 01-4 4zm0 0h12a2 2 0 002-2v-4a2 2 0 00-2-2h-2.343M11 7.343l1.657-1.657a2 2 0 012.828 0l2.829 2.829a2 2 0 010 2.828l-8.486 8.485M7 17h.01"
              />
            </svg>
            <span className="sr-only">Select theme</span>
          </div>
          <ul
            tabIndex={0}
            className="dropdown-content z-[1] menu p-2 shadow bg-base-100 rounded-box w-40"
          >
            {themes.map((t) => (
              <li key={t}>
                <button
                  onClick={() => handleThemeChange(t)}
                  className={theme === t ? 'active' : ''}
                >
                  {t.charAt(0).toUpperCase() + t.slice(1)}
                </button>
              </li>
            ))}
          </ul>
        </div>

        {/* Authenticated user: Show Dashboard link */}
        {isAuthenticated && (
          <Link to="/dashboard" className="btn btn-ghost min-h-[44px]">
            Dashboard
          </Link>
        )}

        {/* Unauthenticated user: Show Login and Register links */}
        {!isAuthenticated && (
          <>
            <Link to="/login" className="btn btn-ghost min-h-[44px]">
              Login
            </Link>
            <Link to="/register" className="btn btn-primary min-h-[44px]">
              Register
            </Link>
          </>
        )}
      </div>

      {/* Mobile hamburger menu button - visible only on mobile */}
      <div className="navbar-end md:hidden">
        <button
          type="button"
          onClick={toggleMobileMenu}
          className="btn btn-ghost min-h-[44px] min-w-[44px]"
          aria-label="Toggle menu"
          aria-expanded={isMobileMenuOpen}
          data-testid="mobile-menu-button"
        >
          {isMobileMenuOpen ? (
            // Close icon
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-6 w-6"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M6 18L18 6M6 6l12 12"
              />
            </svg>
          ) : (
            // Hamburger icon
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-6 w-6"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M4 6h16M4 12h16M4 18h16"
              />
            </svg>
          )}
        </button>
      </div>

      {/* Mobile menu - slides in from top */}
      {isMobileMenuOpen && (
        <div
          className="absolute top-full left-0 right-0 bg-base-200 shadow-lg md:hidden z-50"
          data-testid="mobile-menu"
          role="menu"
        >
          <ul className="menu p-4 gap-2">
            {/* Theme selector in mobile menu */}
            <li>
              <details>
                <summary className="min-h-[44px] flex items-center">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-5 w-5 mr-2"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    aria-hidden="true"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M7 21a4 4 0 01-4-4V5a2 2 0 012-2h4a2 2 0 012 2v12a4 4 0 01-4 4zm0 0h12a2 2 0 002-2v-4a2 2 0 00-2-2h-2.343M11 7.343l1.657-1.657a2 2 0 012.828 0l2.829 2.829a2 2 0 010 2.828l-8.486 8.485M7 17h.01"
                    />
                  </svg>
                  Theme
                </summary>
                <ul className="bg-base-100 rounded-box">
                  {themes.map((t) => (
                    <li key={t}>
                      <button
                        onClick={() => {
                          handleThemeChange(t)
                          closeMobileMenu()
                        }}
                        className={theme === t ? 'active' : ''}
                      >
                        {t.charAt(0).toUpperCase() + t.slice(1)}
                      </button>
                    </li>
                  ))}
                </ul>
              </details>
            </li>

            {/* Authenticated user: Show Dashboard link */}
            {isAuthenticated && (
              <li>
                <Link
                  to="/dashboard"
                  onClick={closeMobileMenu}
                  className="min-h-[44px] flex items-center"
                >
                  Dashboard
                </Link>
              </li>
            )}

            {/* Unauthenticated user: Show Login and Register links */}
            {!isAuthenticated && (
              <>
                <li>
                  <Link
                    to="/login"
                    onClick={closeMobileMenu}
                    className="min-h-[44px] flex items-center"
                  >
                    Login
                  </Link>
                </li>
                <li>
                  <Link
                    to="/register"
                    onClick={closeMobileMenu}
                    className="btn btn-primary min-h-[44px] flex items-center justify-center"
                  >
                    Register
                  </Link>
                </li>
              </>
            )}
          </ul>
        </div>
      )}
    </nav>
  )
}

export default Navbar
