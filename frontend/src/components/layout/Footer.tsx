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
    <footer className="footer footer-center p-10 bg-base-200 text-base-content">
      <nav className="grid grid-flow-col gap-4">
        <Link to="/" className="link link-hover">Home</Link>
        <Link to="/login" className="link link-hover">Login</Link>
        <Link to="/register" className="link link-hover">Register</Link>
      </nav>
      <nav className="grid grid-flow-col gap-4">
        <Link to="/privacy" className="link link-hover">Privacy Policy</Link>
        <Link to="/terms" className="link link-hover">Terms of Service</Link>
      </nav>
      <aside>
        <p>Copyright &copy; {currentYear} URLShort. All rights reserved.</p>
      </aside>
    </footer>
  )
}
