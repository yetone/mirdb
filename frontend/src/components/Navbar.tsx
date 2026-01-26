import { useState } from 'react'
import { Link } from 'react-router-dom'
import { FuturisticButton } from './FuturisticButton'
import { ThemeToggle } from './ThemeToggle'

interface NavbarProps {
  onGetStarted?: () => void
  onSignIn?: () => void
}

export function Navbar({ onGetStarted, onSignIn }: NavbarProps) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen)
  }

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false)
  }

  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-md sticky top-0 z-50 border-b border-base-300/50">
      <div className="container mx-auto px-4">
        <div className="flex-1">
          <Link to="/" className="btn btn-ghost text-xl font-bold">
            🔗 URLShortener
          </Link>
        </div>

        {/* Desktop Navigation */}
        <div className="hidden md:flex items-center gap-4" data-testid="desktop-nav">
          <a href="#features" className="btn btn-ghost btn-sm">
            Features
          </a>
          <a href="#how-it-works" className="btn btn-ghost btn-sm">
            How It Works
          </a>
        </div>

        {/* Desktop Auth Buttons and Theme Toggle */}
        <div className="hidden md:flex items-center gap-2 ml-4">
          <ThemeToggle />
          <FuturisticButton
            variant="outline"
            size="sm"
            onClick={onSignIn}
            data-testid="sign-in-nav"
          >
            Sign In
          </FuturisticButton>
          <FuturisticButton
            variant="primary"
            size="sm"
            onClick={onGetStarted}
            data-testid="get-started-nav"
          >
            Get Started
          </FuturisticButton>
        </div>

        {/* Mobile Hamburger Button */}
        <button
          className="md:hidden btn btn-ghost btn-square min-h-[44px] min-w-[44px]"
          onClick={toggleMobileMenu}
          aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
          aria-expanded={isMobileMenuOpen}
          aria-controls="mobile-menu"
          data-testid="hamburger-menu"
        >
          {isMobileMenuOpen ? (
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth={1.5}
              stroke="currentColor"
              className="w-6 h-6"
              aria-hidden="true"
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          ) : (
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth={1.5}
              stroke="currentColor"
              className="w-6 h-6"
              aria-hidden="true"
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25h16.5" />
            </svg>
          )}
        </button>
      </div>

      {/* Mobile Menu Drawer */}
      {isMobileMenuOpen && (
        <div
          id="mobile-menu"
          className="md:hidden absolute top-full left-0 right-0 bg-base-100/95 backdrop-blur-md border-b border-base-300/50 shadow-lg"
          data-testid="mobile-menu"
        >
          <div className="container mx-auto px-4 py-4 flex flex-col gap-2">
            <a
              href="#features"
              className="btn btn-ghost justify-start min-h-[44px]"
              onClick={closeMobileMenu}
              data-testid="mobile-nav-features"
            >
              Features
            </a>
            <a
              href="#how-it-works"
              className="btn btn-ghost justify-start min-h-[44px]"
              onClick={closeMobileMenu}
              data-testid="mobile-nav-how-it-works"
            >
              How It Works
            </a>
            <div className="divider my-2"></div>
            <div className="flex items-center justify-between px-2">
              <span className="text-sm">Theme</span>
              <ThemeToggle />
            </div>
            <div className="divider my-2"></div>
            <FuturisticButton
              variant="outline"
              size="md"
              onClick={() => {
                closeMobileMenu()
                onSignIn?.()
              }}
              data-testid="mobile-sign-in"
              className="min-h-[44px]"
            >
              Sign In
            </FuturisticButton>
            <FuturisticButton
              variant="primary"
              size="md"
              onClick={() => {
                closeMobileMenu()
                onGetStarted?.()
              }}
              data-testid="mobile-get-started"
              className="min-h-[44px]"
            >
              Get Started
            </FuturisticButton>
          </div>
        </div>
      )}
    </nav>
  )
}
