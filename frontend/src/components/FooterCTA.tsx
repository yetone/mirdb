import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'
import FuturisticButton from './FuturisticButton'

interface FooterCTAProps {
  'data-testid'?: string
}

const navigationLinks = [
  { name: 'Home', to: '/', testId: 'footer-nav-home' },
  { name: 'Login', to: '/login', testId: 'footer-nav-login' },
  { name: 'Register', to: '/register', testId: 'footer-nav-register' },
  { name: 'Dashboard', to: '/dashboard', testId: 'footer-nav-dashboard' },
]

const legalLinks = [
  { name: 'Terms of Service', to: '/terms', testId: 'footer-legal-terms' },
  { name: 'Privacy Policy', to: '/privacy', testId: 'footer-legal-privacy' },
]

export default function FooterCTA({ 'data-testid': testId }: FooterCTAProps) {
  return (
    <footer
      data-testid={testId || 'footer-cta'}
      className="bg-base-200 py-16"
    >
      <div className="container mx-auto px-4 text-center">
        <motion.h2
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-2xl md:text-3xl lg:text-4xl font-bold text-base-content mb-4"
          data-testid="footer-headline"
        >
          Ready to Get Started?
        </motion.h2>

        <motion.p
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.1 }}
          className="text-base-content/70 text-lg mb-8 max-w-xl mx-auto"
          data-testid="footer-subheadline"
        >
          Join thousands of users who trust us to shorten and track their links.
        </motion.p>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.2 }}
        >
          <FuturisticButton
            as="link"
            to="/register"
            variant="primary"
            size="lg"
            data-testid="footer-cta-get-started"
          >
            Get Started Free
          </FuturisticButton>
        </motion.div>

        <motion.div
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.3 }}
          className="mt-12 pt-8 border-t border-base-300"
        >
          <div className="flex flex-col md:flex-row justify-center items-center gap-8 md:gap-16 mb-8">
            <motion.nav
              aria-label="Footer navigation"
              data-testid="footer-navigation"
              className="flex flex-wrap justify-center gap-4 md:gap-6"
            >
              {navigationLinks.map((link) => (
                <Link
                  key={link.to}
                  to={link.to}
                  data-testid={link.testId}
                  className="text-base-content/70 hover:text-primary transition-colors text-sm md:text-base"
                >
                  {link.name}
                </Link>
              ))}
            </motion.nav>

            <div
              data-testid="footer-legal"
              className="flex flex-wrap justify-center gap-4 md:gap-6"
            >
              {legalLinks.map((link) => (
                <Link
                  key={link.to}
                  to={link.to}
                  data-testid={link.testId}
                  className="text-base-content/50 hover:text-primary transition-colors text-sm"
                >
                  {link.name}
                </Link>
              ))}
            </div>
          </div>

          <p className="text-base-content/50 text-sm" data-testid="footer-copyright">
            © {new Date().getFullYear()} URL Shortener. All rights reserved.
          </p>
        </motion.div>
      </div>
    </footer>
  )
}
