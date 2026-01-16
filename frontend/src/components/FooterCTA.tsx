import { motion } from 'framer-motion'
import FuturisticButton from './FuturisticButton'

interface FooterCTAProps {
  'data-testid'?: string
}

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
          <p className="text-base-content/50 text-sm" data-testid="footer-copyright">
            © {new Date().getFullYear()} URL Shortener. All rights reserved.
          </p>
        </motion.div>
      </div>
    </footer>
  )
}
