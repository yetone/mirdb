/**
 * Page footer component.
 * Owner: Scenario 6 - Footer Links and Legal Information
 *
 * Contains:
 * - Navigation links (Home, Login, Register)
 * - Legal links (Privacy Policy, Terms of Service)
 * - Copyright notice
 * - Theme toggle integration
 */

import React from 'react'
import { Link } from 'react-router-dom'

export function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      data-testid="footer"
      role="contentinfo"
      aria-label="Site footer"
      className="footer footer-center p-10 bg-base-200 text-base-content"
    >
      <nav
        data-testid="footer-nav"
        aria-label="Footer navigation"
        className="grid grid-flow-col gap-4"
      >
        <Link to="/" className="link link-hover" data-testid="footer-link-home">
          Home
        </Link>
        <Link to="/login" className="link link-hover" data-testid="footer-link-login">
          Login
        </Link>
        <Link to="/register" className="link link-hover" data-testid="footer-link-register">
          Register
        </Link>
      </nav>
      <nav
        data-testid="footer-legal"
        aria-label="Legal links"
        className="grid grid-flow-col gap-4"
      >
        <Link to="/privacy" className="link link-hover" data-testid="footer-link-privacy">
          Privacy Policy
        </Link>
        <Link to="/terms" className="link link-hover" data-testid="footer-link-terms">
          Terms of Service
        </Link>
      </nav>
      <aside data-testid="footer-copyright">
        <p>Copyright &copy; {currentYear} URLShort. All rights reserved.</p>
      </aside>
    </footer>
  )
}
