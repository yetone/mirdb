/**
 * Header Component
 * Owner: Scenario 2 - Navigation & Header
 *
 * Fixed header containing:
 * - MirDB logo/name (links to top)
 * - Navigation links (Features, Quick Start, Usage, GitHub)
 * - Theme toggle button (placeholder for Scenario 6)
 * - Mobile hamburger menu (< 768px)
 */
import { useState, useCallback, useRef } from 'react'
import { Navigation, MobileMenu } from '../Navigation/Navigation'
import { ThemeToggle } from '@/components/ui/ThemeToggle/ThemeToggle'
import { siteConfig } from '@/data/config'
import styles from './Header.module.css'

export function Header() {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)
  const hamburgerRef = useRef<HTMLButtonElement>(null)

  const handleToggleMobileMenu = useCallback(() => {
    setIsMobileMenuOpen((prev) => !prev)
  }, [])

  const handleCloseMobileMenu = useCallback(() => {
    setIsMobileMenuOpen(false)
    // Return focus to hamburger button for accessibility
    hamburgerRef.current?.focus()
  }, [])

  const handleLogoClick = (e: React.MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault()
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }

  return (
    <header className={styles.header} role="banner">
      <div className={styles.container}>
        {/* Logo/Brand */}
        <a
          href="#"
          className={styles.logo}
          onClick={handleLogoClick}
          aria-label="Go to top of page"
        >
          <img
            src="/assets/logo.gif"
            alt=""
            className={styles.logoImage}
            aria-hidden="true"
          />
          <span className={styles.logoText}>{siteConfig.name}</span>
        </a>

        {/* Desktop Navigation */}
        <Navigation />

        {/* Right side actions */}
        <div className={styles.actions}>
          {/* Theme toggle - implemented by Scenario 6 */}
          <ThemeToggle />

          {/* Mobile hamburger button */}
          <button
            ref={hamburgerRef}
            className={styles.hamburger}
            onClick={handleToggleMobileMenu}
            aria-label={isMobileMenuOpen ? 'Close navigation menu' : 'Open navigation menu'}
            aria-expanded={isMobileMenuOpen}
            aria-controls="mobile-menu"
            data-testid="hamburger-button"
          >
            <span className={styles.hamburgerLine} aria-hidden="true" />
            <span className={styles.hamburgerLine} aria-hidden="true" />
            <span className={styles.hamburgerLine} aria-hidden="true" />
          </button>
        </div>
      </div>

      {/* Mobile Menu */}
      <MobileMenu isOpen={isMobileMenuOpen} onClose={handleCloseMobileMenu} />
    </header>
  )
}
