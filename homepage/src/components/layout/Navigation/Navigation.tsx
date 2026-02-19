/**
 * Navigation Component
 * Owner: Scenario 2 - Navigation & Header
 *
 * Desktop and mobile navigation:
 * - Desktop: horizontal nav links
 * - Mobile: hamburger menu with slide-out drawer
 * - Smooth scroll to sections
 */
import { useEffect, useRef, useCallback } from 'react'
import { navItems } from '@/data/config'
import type { NavItem } from '@/types'
import styles from './Navigation.module.css'

interface NavigationProps {
  isMobile?: boolean
  onNavClick?: () => void
}

export function Navigation({ isMobile = false, onNavClick }: NavigationProps) {
  const handleClick = (e: React.MouseEvent<HTMLAnchorElement>, item: NavItem) => {
    if (!item.external && item.href.startsWith('#')) {
      e.preventDefault()
      const targetId = item.href.slice(1)
      const targetElement = document.getElementById(targetId)
      if (targetElement) {
        targetElement.scrollIntoView({ behavior: 'smooth' })
        onNavClick?.()
      }
    } else if (item.external) {
      onNavClick?.()
    }
  }

  return (
    <nav className={isMobile ? styles.navMobile : styles.nav} aria-label="Main navigation">
      <ul className={styles.navList} role="menubar">
        {navItems.map((item) => (
          <li key={item.label} role="none">
            <a
              href={item.href}
              className={styles.navLink}
              onClick={(e) => handleClick(e, item)}
              target={item.external ? '_blank' : undefined}
              rel={item.external ? 'noopener noreferrer' : undefined}
              role="menuitem"
            >
              {item.label}
              {item.external && (
                <svg
                  className={styles.externalIcon}
                  width="12"
                  height="12"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  aria-hidden="true"
                >
                  <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                  <polyline points="15 3 21 3 21 9" />
                  <line x1="10" y1="14" x2="21" y2="3" />
                </svg>
              )}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  )
}

interface MobileMenuProps {
  isOpen: boolean
  onClose: () => void
}

export function MobileMenu({ isOpen, onClose }: MobileMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null)
  const firstLinkRef = useRef<HTMLAnchorElement>(null)

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (e.key === 'Escape') {
      onClose()
    }
  }, [onClose])

  const handleClickOutside = useCallback((e: MouseEvent) => {
    if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
      onClose()
    }
  }, [onClose])

  useEffect(() => {
    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown)
      document.addEventListener('mousedown', handleClickOutside)
      // Prevent body scroll when menu is open
      document.body.style.overflow = 'hidden'
      // Focus the first link for accessibility
      setTimeout(() => {
        firstLinkRef.current?.focus()
      }, 100)
    } else {
      document.body.style.overflow = ''
    }

    return () => {
      document.removeEventListener('keydown', handleKeyDown)
      document.removeEventListener('mousedown', handleClickOutside)
      document.body.style.overflow = ''
    }
  }, [isOpen, handleKeyDown, handleClickOutside])

  const handleNavClick = (e: React.MouseEvent<HTMLAnchorElement>, item: NavItem) => {
    if (!item.external && item.href.startsWith('#')) {
      e.preventDefault()
      const targetId = item.href.slice(1)
      const targetElement = document.getElementById(targetId)
      if (targetElement) {
        targetElement.scrollIntoView({ behavior: 'smooth' })
        onClose()
      }
    } else if (item.external) {
      onClose()
    }
  }

  return (
    <>
      {/* Backdrop overlay */}
      <div
        className={`${styles.backdrop} ${isOpen ? styles.backdropOpen : ''}`}
        aria-hidden="true"
      />

      {/* Mobile menu drawer */}
      <div
        ref={menuRef}
        className={`${styles.mobileMenu} ${isOpen ? styles.mobileMenuOpen : ''}`}
        aria-hidden={!isOpen}
        role="dialog"
        aria-modal="true"
        aria-label="Mobile navigation menu"
      >
        <nav aria-label="Mobile navigation">
          <ul className={styles.mobileNavList} role="menu">
            {navItems.map((item, index) => (
              <li key={item.label} role="none">
                <a
                  ref={index === 0 ? firstLinkRef : undefined}
                  href={item.href}
                  className={styles.mobileNavLink}
                  onClick={(e) => handleNavClick(e, item)}
                  target={item.external ? '_blank' : undefined}
                  rel={item.external ? 'noopener noreferrer' : undefined}
                  role="menuitem"
                  tabIndex={isOpen ? 0 : -1}
                >
                  {item.label}
                  {item.external && (
                    <svg
                      className={styles.externalIcon}
                      width="14"
                      height="14"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      aria-hidden="true"
                    >
                      <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                      <polyline points="15 3 21 3 21 9" />
                      <line x1="10" y1="14" x2="21" y2="3" />
                    </svg>
                  )}
                </a>
              </li>
            ))}
          </ul>
        </nav>
      </div>
    </>
  )
}
