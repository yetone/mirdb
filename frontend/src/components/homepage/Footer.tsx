/**
 * Footer Component
 * Owner: Scenario 6 - Footer Navigation Links
 *
 * Footer with:
 * - Logo and tagline
 * - Navigation columns: Product, Company, Resources, Legal
 * - Links: About, Features, Pricing, Contact
 * - Copyright notice
 * - Social media icon placeholders
 *
 * Requirements: REQ-8
 */
import { Link } from 'react-router-dom';
import { LinkIcon } from '@heroicons/react/24/outline';
import type { FooterSection } from '../../types/homepage';

const footerNavigation: FooterSection[] = [
  {
    title: 'Product',
    links: [
      { label: 'Features', href: '/features' },
      { label: 'Pricing', href: '/pricing' },
      { label: 'Dashboard', href: '/dashboard' },
      { label: 'API', href: '/api' },
    ],
  },
  {
    title: 'Company',
    links: [
      { label: 'About', href: '/about' },
      { label: 'Blog', href: '/blog' },
      { label: 'Careers', href: '/careers' },
      { label: 'Contact', href: '/contact' },
    ],
  },
  {
    title: 'Resources',
    links: [
      { label: 'Documentation', href: '/docs' },
      { label: 'Help Center', href: '/help' },
      { label: 'Status', href: '/status' },
      { label: 'Changelog', href: '/changelog' },
    ],
  },
  {
    title: 'Legal',
    links: [
      { label: 'Privacy', href: '/privacy' },
      { label: 'Terms', href: '/terms' },
      { label: 'Cookie Policy', href: '/cookies' },
    ],
  },
];

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer
      className="footer footer-center bg-base-200 text-base-content p-10"
      data-testid="footer"
      aria-label="Site footer"
    >
      <div className="container mx-auto">
        {/* Logo and Tagline Section */}
        <div className="mb-8" data-testid="footer-branding">
          <div className="flex items-center justify-center gap-2 mb-2">
            <div className="p-2 bg-primary rounded-lg">
              <LinkIcon className="w-6 h-6 text-primary-content" aria-hidden="true" />
            </div>
            <span className="text-xl font-bold" data-testid="footer-logo">
              ShortLink
            </span>
          </div>
          <p className="text-sm text-base-content/70" data-testid="footer-tagline">
            Shorten links. Track clicks. Grow your reach.
          </p>
        </div>

        {/* Navigation Columns */}
        <div
          className="grid grid-cols-2 md:grid-cols-4 gap-8 text-left w-full max-w-4xl mb-8"
          data-testid="footer-navigation"
        >
          {footerNavigation.map((section) => (
            <nav key={section.title} aria-label={`${section.title} navigation`}>
              <h3 className="footer-title text-base-content opacity-90 mb-3">
                {section.title}
              </h3>
              <ul className="space-y-2">
                {section.links.map((link) => (
                  <li key={link.label}>
                    <Link
                      to={link.href}
                      className="link link-hover text-sm"
                      data-testid={`footer-link-${link.label.toLowerCase()}`}
                    >
                      {link.label}
                    </Link>
                  </li>
                ))}
              </ul>
            </nav>
          ))}
        </div>

        {/* Social Media Icons */}
        <div className="mb-6" data-testid="footer-social">
          <h3 className="sr-only">Social Media</h3>
          <div className="flex justify-center gap-4">
            <a
              href="https://twitter.com"
              target="_blank"
              rel="noopener noreferrer"
              className="btn btn-ghost btn-circle"
              aria-label="Twitter"
              data-testid="social-twitter"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24"
                className="fill-current"
                aria-hidden="true"
              >
                <path d="M24 4.557c-.883.392-1.832.656-2.828.775 1.017-.609 1.798-1.574 2.165-2.724-.951.564-2.005.974-3.127 1.195-.897-.957-2.178-1.555-3.594-1.555-3.179 0-5.515 2.966-4.797 6.045-4.091-.205-7.719-2.165-10.148-5.144-1.29 2.213-.669 5.108 1.523 6.574-.806-.026-1.566-.247-2.229-.616-.054 2.281 1.581 4.415 3.949 4.89-.693.188-1.452.232-2.224.084.626 1.956 2.444 3.379 4.6 3.419-2.07 1.623-4.678 2.348-7.29 2.04 2.179 1.397 4.768 2.212 7.548 2.212 9.142 0 14.307-7.721 13.995-14.646.962-.695 1.797-1.562 2.457-2.549z" />
              </svg>
            </a>
            <a
              href="https://github.com"
              target="_blank"
              rel="noopener noreferrer"
              className="btn btn-ghost btn-circle"
              aria-label="GitHub"
              data-testid="social-github"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24"
                className="fill-current"
                aria-hidden="true"
              >
                <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z" />
              </svg>
            </a>
            <a
              href="https://linkedin.com"
              target="_blank"
              rel="noopener noreferrer"
              className="btn btn-ghost btn-circle"
              aria-label="LinkedIn"
              data-testid="social-linkedin"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24"
                className="fill-current"
                aria-hidden="true"
              >
                <path d="M4.98 3.5c0 1.381-1.11 2.5-2.48 2.5s-2.48-1.119-2.48-2.5c0-1.38 1.11-2.5 2.48-2.5s2.48 1.12 2.48 2.5zm.02 4.5h-5v16h5v-16zm7.982 0h-4.968v16h4.969v-8.399c0-4.67 6.029-5.052 6.029 0v8.399h4.988v-10.131c0-7.88-8.922-7.593-11.018-3.714v-2.155z" />
              </svg>
            </a>
          </div>
        </div>

        {/* Copyright Notice */}
        <div className="border-t border-base-300 pt-6 w-full">
          <p className="text-sm text-base-content/60" data-testid="footer-copyright">
            &copy; {currentYear} ShortLink. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  );
}
