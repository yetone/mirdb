/**
 * Homepage Footer Component
 * Owner: Scenario 6 - Footer Display
 *
 * Footer section for the homepage.
 *
 * Expected exports:
 * - Footer: React.FC - The footer component
 *
 * Requirements:
 * - Copyright text
 * - Relevant links (optional)
 * - Consistent with overall theme
 * - Semantic HTML (footer element)
 */

import React from 'react'
import { Link } from 'react-router-dom'

export const Footer: React.FC = () => {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="footer footer-center p-10 bg-base-200 text-base-content"
      data-testid="footer"
      aria-label="Site footer"
    >
      {/* Footer Links */}
      <div className="grid grid-flow-col gap-4" data-testid="footer-links">
        <Link to="/login" className="link link-hover" data-testid="footer-link-login">
          Login
        </Link>
        <Link to="/register" className="link link-hover" data-testid="footer-link-register">
          Register
        </Link>
      </div>

      {/* Copyright */}
      <div data-testid="footer-copyright">
        <p className="text-base-content/80">
          Copyright &copy; {currentYear} URL Shortener. All rights reserved.
        </p>
      </div>
    </footer>
  )
}

export default Footer
