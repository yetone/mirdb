/**
 * Footer Component.
 * Owner: Scenario 9 - Footer Component
 *
 * Displays:
 * - Copyright notice
 * - Links to login/register
 * - Optional social links placeholder
 *
 * Requirements:
 * - REQ-9: Include footer with essential links and copyright
 */

import React from 'react'
import { Link } from 'react-router-dom'
import type { FooterProps } from '@/types/homepage'

export function Footer({ copyrightYear = new Date().getFullYear() }: FooterProps) {
  return (
    <footer className="footer footer-center p-10 bg-base-300 text-base-content" role="contentinfo">
      <nav className="grid grid-flow-col gap-4" aria-label="Footer navigation">
        <Link to="/login" className="link link-hover">
          Login
        </Link>
        <Link to="/register" className="link link-hover">
          Register
        </Link>
      </nav>
      <aside>
        <p>&copy; {copyrightYear} URL Shortener. All rights reserved.</p>
      </aside>
    </footer>
  )
}
