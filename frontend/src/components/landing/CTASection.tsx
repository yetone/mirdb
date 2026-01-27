/**
 * CTA Section Component
 * Owner: Scenario 10 - Call-to-Action Section
 *
 * Final conversion section before footer:
 * - Compelling headline asking user to take action
 * - Registration CTA button
 * - Reassurance text (e.g., "Free to get started", "No credit card required")
 *
 * Uses:
 * - FuturisticButton for CTA
 * - Framer Motion for entrance animation
 * - Tailwind CSS for styling
 *
 * Props:
 * - onRegister?: () => void - Callback for registration CTA
 */
import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import FuturisticButton from '../FuturisticButton';

interface CTASectionProps {
  onRegister?: () => void;
}

export default function CTASection({ onRegister }: CTASectionProps) {
  const navigate = useNavigate();

  const handleRegister = () => {
    if (onRegister) {
      onRegister();
    } else {
      navigate('/register');
    }
  };

  return (
    <section
      className="py-20 px-4"
      aria-labelledby="cta-heading"
      data-testid="cta-section"
    >
      <div className="max-w-4xl mx-auto text-center">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6 }}
        >
          <h2
            id="cta-heading"
            className="text-3xl md:text-4xl font-bold mb-4 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
          >
            Ready to Start Shortening Links?
          </h2>

          <p className="text-lg text-base-content/80 mb-8 max-w-2xl mx-auto">
            Join thousands of users who trust our platform for their URL management needs.
            Get started today and take control of your links.
          </p>

          <FuturisticButton
            variant="primary"
            size="lg"
            onClick={handleRegister}
            aria-label="Create a free account"
            data-testid="cta-register-button"
          >
            Create Free Account
          </FuturisticButton>

          <p className="mt-6 text-sm text-base-content/60" data-testid="reassurance-text">
            Free to get started • No credit card required
          </p>
        </motion.div>
      </div>
    </section>
  );
}
