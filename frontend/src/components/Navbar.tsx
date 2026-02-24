/**
 * Navigation Bar Component
 * Owner: Scenario 7 - Navigation Bar
 *
 * Displays the navigation bar with:
 * - Logo/brand link to homepage
 * - Login and Register links (for unauthenticated users)
 * - Dashboard link (for authenticated users only)
 * - Theme toggle
 *
 * Uses AuthContext to determine which links to display based on authentication state.
 */

import { Link } from 'react-router-dom'
import { useAuth } from '@/contexts/AuthContext'
import { useTheme } from '@/contexts/ThemeContext'

export function Navbar() {
  const { isAuthenticated } = useAuth()
  const { theme, setTheme, themes } = useTheme()

  const handleThemeChange = (newTheme: string) => {
    setTheme(newTheme)
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

      <div className="navbar-end gap-2">
        {/* Theme selector dropdown */}
        <div className="dropdown dropdown-end">
          <div tabIndex={0} role="button" className="btn btn-ghost btn-sm">
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
          <Link to="/dashboard" className="btn btn-ghost btn-sm">
            Dashboard
          </Link>
        )}

        {/* Unauthenticated user: Show Login and Register links */}
        {!isAuthenticated && (
          <>
            <Link to="/login" className="btn btn-ghost btn-sm">
              Login
            </Link>
            <Link to="/register" className="btn btn-primary btn-sm">
              Register
            </Link>
          </>
        )}
      </div>
    </nav>
  )
}

export default Navbar
