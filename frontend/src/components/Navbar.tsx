/**
 * Navbar Component
 * Owner: Scenario 2 - Navigation and CTA Buttons
 *
 * Navigation bar with links to Features, FAQ, Login, and Register.
 * Includes anchor navigation for on-page sections and route navigation.
 *
 * Requirements:
 * - REQ-2: Prominent CTA button for registration
 * - REQ-4: Quick-link button for login
 * - REQ-9: Navigation links to key sections
 */
import { Link, useNavigate, useLocation } from 'react-router-dom'
import { motion } from 'framer-motion'
import { FuturisticButton } from './FuturisticButton'
import { ThemeToggle } from './ThemeToggle'

export interface NavbarProps {
  showAuthButtons?: boolean
}

export function Navbar({ showAuthButtons = true }: NavbarProps) {
  const navigate = useNavigate()
  const location = useLocation()

  const scrollToSection = (sectionId: string) => {
    // If we're not on the homepage, navigate there first
    if (location.pathname !== '/') {
      navigate('/')
      // Wait for navigation then scroll
      setTimeout(() => {
        const element = document.getElementById(sectionId)
        element?.scrollIntoView({ behavior: 'smooth' })
      }, 100)
    } else {
      const element = document.getElementById(sectionId)
      element?.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <motion.nav
      className="navbar bg-base-100/80 backdrop-blur-md fixed top-0 left-0 right-0 z-50 px-4 sm:px-6 lg:px-8"
      data-testid="navbar"
      role="navigation"
      aria-label="Main navigation"
      initial={{ y: -100 }}
      animate={{ y: 0 }}
      transition={{ duration: 0.5, ease: 'easeOut' }}
    >
      <div className="flex-1">
        <Link
          to="/"
          className="text-xl font-bold"
          data-testid="navbar-logo"
          aria-label="Home"
        >
          URL Shortener
        </Link>
      </div>

      {/* Desktop Navigation */}
      <div className="hidden md:flex items-center gap-6">
        <button
          onClick={() => scrollToSection('features-section')}
          className="link link-hover text-base-content/80 hover:text-primary transition-colors"
          data-testid="nav-features"
          aria-label="Navigate to Features section"
        >
          Features
        </button>
        <button
          onClick={() => scrollToSection('faq-section')}
          className="link link-hover text-base-content/80 hover:text-primary transition-colors"
          data-testid="nav-faq"
          aria-label="Navigate to FAQ section"
        >
          FAQ
        </button>

        {showAuthButtons && (
          <>
            <Link
              to="/login"
              className="link link-hover text-base-content/80 hover:text-primary transition-colors"
              data-testid="nav-login"
              aria-label="Login to your account"
            >
              Login
            </Link>
            <FuturisticButton
              variant="primary"
              size="sm"
              onClick={() => navigate('/register')}
              data-testid="nav-register"
              aria-label="Get Started - Create an account"
            >
              Get Started
            </FuturisticButton>
          </>
        )}
        <ThemeToggle data-testid="navbar-theme-toggle" />
      </div>

      {/* Mobile Navigation - Dropdown */}
      <div className="md:hidden">
        <div className="dropdown dropdown-end">
          <label
            tabIndex={0}
            className="btn btn-ghost"
            aria-label="Open navigation menu"
            data-testid="mobile-menu-toggle"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-6 w-6"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M4 6h16M4 12h16M4 18h16"
              />
            </svg>
          </label>
          <ul
            tabIndex={0}
            className="dropdown-content menu p-2 shadow bg-base-100 rounded-box w-52"
            role="menu"
            data-testid="mobile-menu"
          >
            <li role="menuitem">
              <button
                onClick={() => scrollToSection('features-section')}
                data-testid="mobile-nav-features"
              >
                Features
              </button>
            </li>
            <li role="menuitem">
              <button
                onClick={() => scrollToSection('faq-section')}
                data-testid="mobile-nav-faq"
              >
                FAQ
              </button>
            </li>
            {showAuthButtons && (
              <>
                <li role="menuitem">
                  <Link to="/login" data-testid="mobile-nav-login">
                    Login
                  </Link>
                </li>
                <li role="menuitem">
                  <Link to="/register" data-testid="mobile-nav-register">
                    Get Started
                  </Link>
                </li>
              </>
            )}
            <li role="menuitem">
              <ThemeToggle showLabel data-testid="mobile-theme-toggle" />
            </li>
          </ul>
        </div>
      </div>
    </motion.nav>
  )
}

export default Navbar
