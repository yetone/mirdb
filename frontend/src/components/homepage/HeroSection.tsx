/**
 * Hero Section Component.
 * Owner: Scenario 2 - Hero Section Display and CTA
 *
 * Displays the main value proposition with prominent CTA buttons.
 * Adapts based on authentication state.
 */
import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import FuturisticButton from '../FuturisticButton';

export interface HeroSectionProps {
  isAuthenticated?: boolean;
  username?: string;
}

export function HeroSection({ isAuthenticated = false, username }: HeroSectionProps) {
  const navigate = useNavigate();

  const handlePrimaryCTA = () => {
    if (isAuthenticated) {
      navigate('/dashboard');
    } else {
      navigate('/register');
    }
  };

  const handleSecondaryCTA = () => {
    const featuresSection = document.getElementById('features');
    if (featuresSection) {
      featuresSection.scrollIntoView({ behavior: 'smooth' });
    }
  };

  return (
    <section
      data-testid="hero-section"
      className="min-h-[80vh] flex items-center justify-center px-4 py-16"
    >
      <div className="max-w-4xl mx-auto text-center">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          {isAuthenticated && username && (
            <motion.p
              className="text-lg text-primary mb-4"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.2 }}
              data-testid="hero-greeting"
            >
              Welcome back, {username}!
            </motion.p>
          )}

          <h1
            data-testid="hero-headline"
            className="text-4xl md:text-6xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
          >
            Shorten Links. Track Clicks.
          </h1>

          <p
            data-testid="hero-subheadline"
            className="text-xl md:text-2xl text-base-content/70 mb-10 max-w-2xl mx-auto"
          >
            Simple & Powerful URL Manager. Create short links and gain insights with detailed analytics.
          </p>

          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
            <FuturisticButton
              variant="primary"
              size="lg"
              onClick={handlePrimaryCTA}
              data-testid="hero-primary-cta"
              aria-label={isAuthenticated ? 'Go to Dashboard' : 'Get Started Free'}
            >
              {isAuthenticated ? 'Go to Dashboard' : 'Get Started Free'}
            </FuturisticButton>

            <FuturisticButton
              variant="ghost"
              size="lg"
              onClick={handleSecondaryCTA}
              data-testid="hero-secondary-cta"
              aria-label="Learn More"
            >
              Learn More
            </FuturisticButton>
          </div>
        </motion.div>
      </div>
    </section>
  );
}

export default HeroSection;
