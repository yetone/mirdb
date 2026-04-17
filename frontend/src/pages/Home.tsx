/**
 * Home Page
 * Owner: Scenario 7 - Theme Support and Consistency
 * Co-owner: Scenario 11 - Navigation Integration
 *
 * Landing page that displays the homepage sections.
 * Integrates with theme system and routes to other pages.
 *
 * Theme Support:
 * - Uses DaisyUI theme classes (bg-base-100, text-base-content, etc.)
 * - Responds to ThemeContext changes via data-theme attribute
 * - All sections inherit theme from document root
 *
 * Navigation Integration:
 * - Shows login link for unauthenticated users
 * - Shows dashboard link for authenticated users
 * - Handles auth state via AuthContext
 *
 * Related requirements: REQ-9, NFR-5, US-6, REQ-3, REQ-8
 */

import { Link } from 'react-router-dom'
import {
  HeroSection,
  HowItWorksSection,
  FeaturesSection,
  SocialProofSection,
  Footer,
  UrlShortenerForm,
} from '../components/homepage'
import { ThemeToggle } from '../components/ThemeToggle'
import { useAuth } from '../contexts/AuthContext'
import { LogIn, LayoutDashboard, UserPlus } from 'lucide-react'

export default function Home() {
  const { isAuthenticated } = useAuth()

  return (
    <main
      className="min-h-screen bg-base-100"
      data-testid="home-page"
    >
      {/* Navigation Header - Auth-aware navigation */}
      <header
        className="fixed top-0 left-0 right-0 z-50 bg-base-100/80 backdrop-blur-md border-b border-base-200"
        data-testid="navigation-header"
      >
        <nav className="container mx-auto px-4 py-3 flex items-center justify-between">
          {/* Logo/Brand */}
          <Link
            to="/"
            className="text-xl font-bold text-primary"
            data-testid="nav-logo"
          >
            LinkShort
          </Link>

          {/* Navigation Links */}
          <div className="flex items-center gap-4">
            {isAuthenticated ? (
              /* Authenticated User Navigation */
              <Link
                to="/dashboard"
                className="btn btn-primary btn-sm gap-2"
                data-testid="nav-dashboard-link"
              >
                <LayoutDashboard className="w-4 h-4" />
                Go to Dashboard
              </Link>
            ) : (
              /* Unauthenticated User Navigation */
              <>
                <Link
                  to="/login"
                  className="btn btn-ghost btn-sm gap-2"
                  data-testid="nav-login-link"
                >
                  <LogIn className="w-4 h-4" />
                  Login
                </Link>
                <Link
                  to="/register"
                  className="btn btn-primary btn-sm gap-2"
                  data-testid="nav-register-link"
                >
                  <UserPlus className="w-4 h-4" />
                  Sign Up
                </Link>
              </>
            )}
          </div>
        </nav>
      </header>

      {/* Theme Toggle - Fixed position for easy access */}
      <div className="fixed top-4 right-4 z-[60]" data-testid="theme-toggle-container">
        <ThemeToggle />
      </div>

      {/* Spacer for fixed header */}
      <div className="h-16" />

      {/* Hero Section - Above the fold */}
      <HeroSection />

      {/* URL Shortener Demo Section - Scenario 2 */}
      <section className="py-16 px-4 bg-base-200/50" data-testid="url-shortener-section">
        <div className="container mx-auto">
          <h2 className="text-2xl md:text-3xl font-bold text-center mb-8 text-base-content">
            Try it now - no account needed
          </h2>
          <UrlShortenerForm />
        </div>
      </section>

      {/* Features Section */}
      <FeaturesSection />

      {/* How It Works Section */}
      <HowItWorksSection />

      {/* Social Proof Section */}
      <SocialProofSection />

      {/* Footer */}
      <Footer />
    </main>
  )
}
