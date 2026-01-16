import { useState } from 'react'
import { Link } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import FuturisticButton from './FuturisticButton'
import ThemeToggle from './ThemeToggle'
import { useAuth } from '../contexts/AuthContext'

interface NavbarProps {
  'data-testid'?: string
}

export default function Navbar({ 'data-testid': testId }: NavbarProps) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)
  const [isUserMenuOpen, setIsUserMenuOpen] = useState(false)
  const { isAuthenticated, user, logout } = useAuth()

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen)
  }

  const handleLogout = () => {
    logout()
    setIsUserMenuOpen(false)
  }

  return (
    <motion.header
      initial={{ opacity: 0, y: -20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
      data-testid={testId || 'navbar'}
      className="fixed top-0 left-0 right-0 z-50 bg-base-100/80 backdrop-blur-md border-b border-base-300"
    >
      <nav className="container mx-auto px-4 py-3">
        <div className="flex items-center justify-between">
          {/* Logo / Brand */}
          <Link
            to="/"
            className="text-xl font-bold text-base-content hover:text-primary transition-colors"
            data-testid="navbar-logo"
          >
            URL Shortener
          </Link>

          {/* Desktop Navigation */}
          <div
            className="hidden md:flex items-center gap-6"
            data-testid="desktop-nav"
          >
            <a
              href="#features"
              className="text-base-content/70 hover:text-primary transition-colors min-h-[44px] flex items-center"
              data-testid="nav-features-desktop"
            >
              Features
            </a>
            <a
              href="#how-it-works"
              className="text-base-content/70 hover:text-primary transition-colors min-h-[44px] flex items-center"
              data-testid="nav-how-it-works-desktop"
            >
              How It Works
            </a>

            {/* Theme Toggle */}
            <ThemeToggle data-testid="navbar-theme-toggle" />

            {/* Conditional Auth Navigation */}
            {isAuthenticated ? (
              <>
                <Link
                  to="/dashboard"
                  className="text-base-content/80 hover:text-primary transition-colors font-medium min-h-[44px] flex items-center"
                  data-testid="navbar-dashboard"
                >
                  Dashboard
                </Link>
                {/* User Menu */}
                <div className="relative">
                  <button
                    onClick={() => setIsUserMenuOpen(!isUserMenuOpen)}
                    className="flex items-center gap-2 px-3 py-2 rounded-lg bg-base-200 hover:bg-base-300 transition-colors min-h-[44px]"
                    aria-expanded={isUserMenuOpen}
                    aria-haspopup="true"
                    data-testid="navbar-user-menu-button"
                  >
                    <div className="w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center">
                      <span className="text-sm font-medium text-primary" data-testid="navbar-user-avatar">
                        {user?.username?.charAt(0).toUpperCase() || 'U'}
                      </span>
                    </div>
                    <span className="text-sm font-medium text-base-content" data-testid="navbar-username">
                      {user?.username || 'User'}
                    </span>
                    <svg
                      className={`w-4 h-4 transition-transform ${isUserMenuOpen ? 'rotate-180' : ''}`}
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                    </svg>
                  </button>
                  <AnimatePresence>
                    {isUserMenuOpen && (
                      <motion.div
                        initial={{ opacity: 0, y: -10 }}
                        animate={{ opacity: 1, y: 0 }}
                        exit={{ opacity: 0, y: -10 }}
                        transition={{ duration: 0.15 }}
                        className="absolute right-0 mt-2 w-48 rounded-lg shadow-lg bg-base-100 border border-base-300 overflow-hidden z-50"
                        data-testid="navbar-user-menu"
                      >
                        <Link
                          to="/profile"
                          className="block px-4 py-3 text-base-content hover:bg-base-200 transition-colors"
                          onClick={() => setIsUserMenuOpen(false)}
                          data-testid="navbar-profile-link"
                        >
                          Profile
                        </Link>
                        <Link
                          to="/settings"
                          className="block px-4 py-3 text-base-content hover:bg-base-200 transition-colors"
                          onClick={() => setIsUserMenuOpen(false)}
                          data-testid="navbar-settings-link"
                        >
                          Settings
                        </Link>
                        <hr className="border-base-300" />
                        <button
                          onClick={handleLogout}
                          className="block w-full text-left px-4 py-3 text-error hover:bg-base-200 transition-colors"
                          data-testid="navbar-logout-button"
                        >
                          Logout
                        </button>
                      </motion.div>
                    )}
                  </AnimatePresence>
                </div>
              </>
            ) : (
              <>
                <Link
                  to="/login"
                  className="text-base-content/80 hover:text-primary transition-colors font-medium min-h-[44px] flex items-center"
                  data-testid="navbar-login"
                >
                  Login
                </Link>
                <Link
                  to="/register"
                  className="bg-primary text-primary-content px-4 py-2 rounded-lg font-medium hover:bg-primary/80 transition-colors min-h-[44px] flex items-center"
                  data-testid="navbar-get-started"
                >
                  Get Started
                </Link>
              </>
            )}
          </div>

          {/* Mobile Hamburger Button */}
          <button
            type="button"
            onClick={toggleMobileMenu}
            className="md:hidden w-11 h-11 flex items-center justify-center rounded-lg hover:bg-base-200 transition-colors"
            aria-expanded={isMobileMenuOpen}
            aria-controls="mobile-menu"
            aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
            data-testid="hamburger-button"
          >
            <span className="sr-only">{isMobileMenuOpen ? 'Close menu' : 'Open menu'}</span>
            <svg
              className="w-6 h-6 text-base-content"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              {isMobileMenuOpen ? (
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M6 18L18 6M6 6l12 12"
                />
              ) : (
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M4 6h16M4 12h16M4 18h16"
                />
              )}
            </svg>
          </button>
        </div>

        {/* Mobile Menu */}
        <AnimatePresence>
          {isMobileMenuOpen && (
            <motion.div
              id="mobile-menu"
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              transition={{ duration: 0.2 }}
              className="md:hidden overflow-hidden"
              data-testid="mobile-menu"
            >
              <div className="py-4 flex flex-col gap-2">
                <a
                  href="#features"
                  onClick={() => setIsMobileMenuOpen(false)}
                  className="block w-full px-4 py-3 min-h-[44px] text-base-content/70 hover:text-primary hover:bg-base-200 rounded-lg transition-colors"
                  data-testid="nav-features-mobile"
                >
                  Features
                </a>
                <a
                  href="#how-it-works"
                  onClick={() => setIsMobileMenuOpen(false)}
                  className="block w-full px-4 py-3 min-h-[44px] text-base-content/70 hover:text-primary hover:bg-base-200 rounded-lg transition-colors"
                  data-testid="nav-how-it-works-mobile"
                >
                  How It Works
                </a>

                {/* Mobile Theme Toggle */}
                <div className="px-4 py-2">
                  <ThemeToggle data-testid="navbar-theme-toggle-mobile" />
                </div>

                <div className="pt-2 border-t border-base-200 mt-2 flex flex-col gap-2">
                  {isAuthenticated ? (
                    <>
                      <FuturisticButton
                        as="link"
                        to="/dashboard"
                        variant="primary"
                        size="md"
                        className="w-full min-h-[44px]"
                        data-testid="nav-dashboard-mobile"
                      >
                        Dashboard
                      </FuturisticButton>
                      <FuturisticButton
                        as="link"
                        to="/profile"
                        variant="outline"
                        size="md"
                        className="w-full min-h-[44px]"
                        data-testid="nav-profile-mobile"
                      >
                        Profile ({user?.username || 'User'})
                      </FuturisticButton>
                      <button
                        onClick={() => {
                          handleLogout()
                          setIsMobileMenuOpen(false)
                        }}
                        className="w-full px-4 py-3 min-h-[44px] text-error hover:bg-base-200 rounded-lg transition-colors text-left"
                        data-testid="nav-logout-mobile"
                      >
                        Logout
                      </button>
                    </>
                  ) : (
                    <>
                      <FuturisticButton
                        as="link"
                        to="/login"
                        variant="outline"
                        size="md"
                        className="w-full min-h-[44px]"
                        data-testid="nav-login-mobile"
                      >
                        Login
                      </FuturisticButton>
                      <FuturisticButton
                        as="link"
                        to="/register"
                        variant="primary"
                        size="md"
                        className="w-full min-h-[44px]"
                        data-testid="nav-register-mobile"
                      >
                        Get Started
                      </FuturisticButton>
                    </>
                  )}
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </nav>
    </motion.header>
  )
}
