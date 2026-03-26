/**
 * Navigation Component
 * Owner: Scenario 4 - Navigation Component
 *
 * Displays navigation bar with:
 * - Logo/brand name linking to homepage
 * - Features link that smooth scrolls to features section
 * - Login button redirecting to /login
 * - Sign Up button redirecting to /register
 * - Sticky/fixed positioning on scroll
 *
 * Requirements: REQ-5, US-4
 */

import { Link, useNavigate } from 'react-router-dom'

export interface NavbarProps {
  onFeaturesClick?: () => void
}

export function Navbar({ onFeaturesClick }: NavbarProps) {
  const navigate = useNavigate()

  const handleFeaturesClick = (e: React.MouseEvent) => {
    e.preventDefault()

    if (onFeaturesClick) {
      onFeaturesClick()
    } else {
      // Default smooth scroll behavior to features section
      const featuresSection = document.getElementById('features-section')
      if (featuresSection) {
        featuresSection.scrollIntoView({ behavior: 'smooth' })
      }
    }
  }

  const handleLoginClick = () => {
    navigate('/login')
  }

  const handleSignUpClick = () => {
    navigate('/register')
  }

  return (
    <nav
      data-testid="navbar"
      className="navbar bg-base-100/95 backdrop-blur-sm fixed top-0 left-0 right-0 z-50 shadow-sm"
      role="navigation"
      aria-label="Main navigation"
    >
      <div className="container mx-auto px-4">
        {/* Logo - Left aligned */}
        <div className="flex-1">
          <Link
            to="/"
            data-testid="navbar-logo"
            className="btn btn-ghost text-xl font-bold normal-case"
            aria-label="Go to homepage"
          >
            LinkShort
          </Link>
        </div>

        {/* Navigation links - Right aligned */}
        <div className="flex-none">
          <ul className="menu menu-horizontal px-1 gap-2">
            <li>
              <a
                href="#features-section"
                data-testid="navbar-features-link"
                onClick={handleFeaturesClick}
                className="font-medium"
              >
                Features
              </a>
            </li>
            <li>
              <button
                data-testid="navbar-login-button"
                onClick={handleLoginClick}
                className="btn btn-ghost"
              >
                Login
              </button>
            </li>
            <li>
              <button
                data-testid="navbar-signup-button"
                onClick={handleSignUpClick}
                className="btn btn-primary"
              >
                Sign Up
              </button>
            </li>
          </ul>
        </div>
      </div>
    </nav>
  )
}

export default Navbar
