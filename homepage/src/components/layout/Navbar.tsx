import React, { useState } from 'react';
import { Container } from '@/components/common/Container';
import { MobileMenu } from './MobileMenu';
import { NAV_ITEMS, GITHUB_URL } from '@/utils/constants';
import styles from './Navbar.module.css';

export function Navbar() {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

  const handleNavClick = (e: React.MouseEvent<HTMLAnchorElement>, href: string, external?: boolean) => {
    if (external) return; // Let external links behave normally

    if (href === '#') {
      e.preventDefault();
      window.scrollTo({ top: 0, behavior: 'smooth' });
      return;
    }

    if (href.startsWith('#')) {
      e.preventDefault();
      const elementId = href.slice(1);
      const element = document.getElementById(elementId);
      if (element) {
        element.scrollIntoView({ behavior: 'smooth' });
      }
    }
  };

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen);
  };

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false);
  };

  return (
    <header className={styles.header} role="banner">
      <nav className={styles.nav} aria-label="Main navigation">
        <Container className={styles.container}>
          <a href="#" className={styles.logo} aria-label="MirDB Home" onClick={(e) => handleNavClick(e, '#')}>
            <span className={styles.logoText}>MirDB</span>
          </a>

          <ul className={styles.navList} role="menubar">
            {NAV_ITEMS.map((item) => (
              <li key={item.label} role="none">
                <a
                  href={item.href}
                  className={styles.navLink}
                  role="menuitem"
                  target={item.external ? '_blank' : undefined}
                  rel={item.external ? 'noopener noreferrer' : undefined}
                  onClick={(e) => handleNavClick(e, item.href, item.external)}
                  data-testid={`nav-link-${item.label.toLowerCase()}`}
                >
                  {item.label}
                  {item.external && (
                    <svg
                      className={styles.externalIcon}
                      width="12"
                      height="12"
                      viewBox="0 0 12 12"
                      fill="none"
                      xmlns="http://www.w3.org/2000/svg"
                      aria-hidden="true"
                    >
                      <path
                        d="M10.5 1.5L1.5 10.5M10.5 1.5H4.5M10.5 1.5V7.5"
                        stroke="currentColor"
                        strokeWidth="1.5"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      />
                    </svg>
                  )}
                </a>
              </li>
            ))}
          </ul>

          <button
            className={styles.menuButton}
            onClick={toggleMobileMenu}
            aria-expanded={isMobileMenuOpen}
            aria-controls="mobile-menu"
            aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
            data-testid="mobile-menu-button"
          >
            <span className={`${styles.hamburger} ${isMobileMenuOpen ? styles.open : ''}`}>
              <span></span>
              <span></span>
              <span></span>
            </span>
          </button>
        </Container>
      </nav>

      <MobileMenu
        isOpen={isMobileMenuOpen}
        onClose={closeMobileMenu}
        onNavClick={handleNavClick}
      />
    </header>
  );
}
