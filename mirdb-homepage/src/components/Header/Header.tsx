/**
 * Header/Navigation Component.
 * Owner: Scenario 4 - Navigation and Header
 *
 * Features:
 * - MirDB logo display
 * - Navigation links: Features, Usage, Architecture, Resources
 * - Theme toggle integration slot
 * - Responsive hamburger menu for mobile
 * - Sticky header with smooth scroll navigation
 */

import { useState, useCallback, type ReactNode } from 'react';
import { NAVIGATION_LINKS, GITHUB_URL } from '../../utils/constants';
import styles from './Header.module.css';

interface HeaderProps {
  /** Optional theme toggle component to render in header */
  themeToggle?: ReactNode;
}

export function Header({ themeToggle }: HeaderProps) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

  const toggleMobileMenu = useCallback(() => {
    setIsMobileMenuOpen((prev) => !prev);
  }, []);

  const handleNavClick = useCallback(
    (e: React.MouseEvent<HTMLAnchorElement>, href: string) => {
      // Only handle internal anchor links
      if (href.startsWith('#')) {
        e.preventDefault();
        const targetId = href.slice(1);
        const targetElement = document.getElementById(targetId);

        if (targetElement) {
          targetElement.scrollIntoView({
            behavior: 'smooth',
            block: 'start',
          });
        }

        // Close mobile menu after navigation
        setIsMobileMenuOpen(false);
      }
    },
    []
  );

  return (
    <header className={styles.header} role="banner">
      <div className={styles.container}>
        <a href="/" className={styles.logo} aria-label="MirDB Home">
          <img
            src="/assets/logo.gif"
            alt="MirDB Logo"
            className={styles.logoImage}
            width="40"
            height="40"
          />
          <span className={styles.logoText}>MirDB</span>
        </a>

        <nav
          className={`${styles.nav} ${isMobileMenuOpen ? styles.navOpen : ''}`}
          aria-label="Main navigation"
        >
          <ul className={styles.navList} role="list">
            {NAVIGATION_LINKS.map((link) => (
              <li key={link.label} className={styles.navItem}>
                <a
                  href={link.href}
                  className={styles.navLink}
                  onClick={(e) => handleNavClick(e, link.href)}
                  {...(link.isExternal && {
                    target: '_blank',
                    rel: 'noopener noreferrer',
                  })}
                >
                  {link.label}
                </a>
              </li>
            ))}
          </ul>
        </nav>

        <div className={styles.actions}>
          {themeToggle && (
            <div className={styles.themeToggleSlot} data-testid="theme-toggle-slot">
              {themeToggle}
            </div>
          )}

          <a
            href={GITHUB_URL}
            className={styles.githubLink}
            target="_blank"
            rel="noopener noreferrer"
            aria-label="View MirDB on GitHub"
          >
            <svg
              className={styles.githubIcon}
              viewBox="0 0 24 24"
              width="24"
              height="24"
              fill="currentColor"
              aria-hidden="true"
            >
              <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z" />
            </svg>
          </a>

          <button
            className={styles.mobileMenuButton}
            onClick={toggleMobileMenu}
            aria-expanded={isMobileMenuOpen}
            aria-controls="mobile-navigation"
            aria-label={isMobileMenuOpen ? 'Close navigation menu' : 'Open navigation menu'}
          >
            <span className={`${styles.hamburger} ${isMobileMenuOpen ? styles.hamburgerOpen : ''}`}>
              <span className={styles.hamburgerLine} aria-hidden="true" />
              <span className={styles.hamburgerLine} aria-hidden="true" />
              <span className={styles.hamburgerLine} aria-hidden="true" />
            </span>
          </button>
        </div>
      </div>
    </header>
  );
}
