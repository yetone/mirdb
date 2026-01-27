/**
 * CTA Section Component
 * Owner: Scenario 11 - Call-to-Action Section Display
 *
 * Requirements:
 * - Reinforcing message encouraging sign-up
 * - Prominent registration button -> /register
 * - Theme-aware styling using Tailwind/DaisyUI
 * - Framer Motion for animations
 */
import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import FuturisticButton from '../FuturisticButton';

export default function CTASection() {
  return (
    <section
      id="cta"
      className="py-16 md:py-24 bg-base-200"
      aria-labelledby="cta-heading"
    >
      <div className="container mx-auto px-4">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true, margin: '-100px' }}
          transition={{ duration: 0.6 }}
          className="text-center max-w-3xl mx-auto"
        >
          <motion.h2
            id="cta-heading"
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.6, delay: 0.1 }}
            className="text-3xl md:text-4xl font-bold mb-4"
          >
            Ready to Get Started?
          </motion.h2>

          <motion.p
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.6, delay: 0.2 }}
            className="text-lg text-base-content/70 mb-8"
          >
            Join thousands of users who are already shortening URLs and tracking
            their performance. Create your free account today and start growing
            your reach.
          </motion.p>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.6, delay: 0.3 }}
          >
            <Link to="/register" data-testid="cta-register-link">
              <FuturisticButton variant="primary" size="lg">
                Create Free Account
              </FuturisticButton>
            </Link>
          </motion.div>
        </motion.div>
      </div>
    </section>
  );
}
