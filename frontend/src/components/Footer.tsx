import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'

interface FooterProps {
  'data-testid'?: string
}

const navigationLinks = [
  { name: 'Home', to: '/' },
  { name: 'Login', to: '/login' },
  { name: 'Register', to: '/register' },
  { name: 'Dashboard', to: '/dashboard' },
]

const legalLinks = [
  { name: 'Terms of Service', to: '/terms' },
  { name: 'Privacy Policy', to: '/privacy' },
]

export default function Footer({ 'data-testid': testId }: FooterProps) {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      data-testid={testId || 'footer'}
      className="bg-base-200 py-12"
    >
      <div className="container mx-auto px-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 md:gap-12">
          {/* Brand Section */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5 }}
            className="text-center md:text-left"
          >
            <h3
              className="text-xl font-bold text-base-content mb-3"
              data-testid="footer-brand-title"
            >
              URL Shortener
            </h3>
            <p
              className="text-base-content/70 text-sm"
              data-testid="footer-brand-description"
            >
              Shorten, share, and track your links with ease.
            </p>
          </motion.div>

          {/* Navigation Links */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.1 }}
            className="text-center md:text-left"
          >
            <h4
              className="text-lg font-semibold text-base-content mb-3"
              data-testid="footer-nav-title"
            >
              Quick Links
            </h4>
            <nav data-testid="footer-navigation">
              <ul className="space-y-2">
                {navigationLinks.map((link) => (
                  <li key={link.name}>
                    <Link
                      to={link.to}
                      data-testid={`footer-nav-${link.name.toLowerCase()}`}
                      className="text-base-content/70 hover:text-primary transition-colors duration-200 text-sm"
                    >
                      {link.name}
                    </Link>
                  </li>
                ))}
              </ul>
            </nav>
          </motion.div>

          {/* Legal Links */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.2 }}
            className="text-center md:text-left"
          >
            <h4
              className="text-lg font-semibold text-base-content mb-3"
              data-testid="footer-legal-title"
            >
              Legal
            </h4>
            <nav data-testid="footer-legal-links">
              <ul className="space-y-2">
                {legalLinks.map((link) => (
                  <li key={link.name}>
                    <Link
                      to={link.to}
                      data-testid={`footer-legal-${link.name.toLowerCase().replace(/\s+/g, '-')}`}
                      className="text-base-content/70 hover:text-primary transition-colors duration-200 text-sm"
                    >
                      {link.name}
                    </Link>
                  </li>
                ))}
              </ul>
            </nav>
          </motion.div>
        </div>

        {/* Copyright Notice */}
        <motion.div
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.3 }}
          className="mt-10 pt-8 border-t border-base-300 text-center"
        >
          <p
            className="text-base-content/50 text-sm"
            data-testid="footer-copyright"
          >
            © {currentYear} URL Shortener. All rights reserved.
          </p>
        </motion.div>
      </div>
    </footer>
  )
}
