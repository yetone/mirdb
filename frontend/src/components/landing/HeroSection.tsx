/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the hero section of the landing page with:
 * - Main headline communicating value proposition
 * - Supporting subheadline
 * - Primary CTA button (Get Started)
 * - Secondary CTA link (Learn More - scrolls to features)
 */
import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import FuturisticButton from '../FuturisticButton';

interface HeroSectionProps {
  onGetStarted?: () => void;
  onLearnMore?: () => void;
}

export default function HeroSection({ onGetStarted, onLearnMore }: HeroSectionProps) {
  const navigate = useNavigate();

  const handleGetStarted = () => {
    if (onGetStarted) {
      onGetStarted();
    } else {
      navigate('/register');
    }
  };

  const handleLearnMore = () => {
    if (onLearnMore) {
      onLearnMore();
    } else {
      const featuresSection = document.getElementById('features');
      if (featuresSection) {
        featuresSection.scrollIntoView({ behavior: 'smooth' });
      }
    }
  };

  return (
    <section
      className="min-h-[80vh] flex items-center justify-center px-4 py-16"
      aria-labelledby="hero-heading"
    >
      <div className="max-w-4xl mx-auto text-center">
        <motion.h1
          id="hero-heading"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
        >
          Shorten Links. Track Everything.
        </motion.h1>

        <motion.p
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
          className="text-lg md:text-xl text-base-content/80 mb-8 max-w-2xl mx-auto"
        >
          Create short, powerful links with detailed analytics. Track clicks,
          understand your audience, and optimize your marketing with our
          easy-to-use URL shortening service.
        </motion.p>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
        >
          <FuturisticButton
            variant="primary"
            size="lg"
            onClick={handleGetStarted}
            aria-label="Get started with URL shortening"
          >
            Get Started
          </FuturisticButton>

          <button
            onClick={handleLearnMore}
            className="link link-hover text-base-content/70 hover:text-primary transition-colors"
            aria-label="Learn more about our features"
          >
            Learn More
          </button>
        </motion.div>

        {/* Visual Element - Dashboard Preview */}
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.8, delay: 0.6 }}
          className="mt-12"
        >
          <div className="mockup-window bg-base-300 shadow-2xl max-w-3xl mx-auto">
            <div className="bg-base-200 p-4">
              <div className="flex items-center gap-2 mb-4">
                <div className="input input-bordered input-sm flex-1 flex items-center px-3 text-sm text-base-content/60">
                  https://example.com/very-long-url-that-needs-shortening
                </div>
                <button className="btn btn-primary btn-sm">Shorten</button>
              </div>
              <div className="grid grid-cols-3 gap-4 text-center">
                <div className="stat bg-base-100 rounded-lg p-3">
                  <div className="stat-title text-xs">Total Links</div>
                  <div className="stat-value text-2xl text-primary">1,234</div>
                </div>
                <div className="stat bg-base-100 rounded-lg p-3">
                  <div className="stat-title text-xs">Total Clicks</div>
                  <div className="stat-value text-2xl text-secondary">56.7K</div>
                </div>
                <div className="stat bg-base-100 rounded-lg p-3">
                  <div className="stat-title text-xs">Active Today</div>
                  <div className="stat-value text-2xl text-accent">89</div>
                </div>
              </div>
            </div>
          </div>
        </motion.div>
      </div>
    </section>
  );
}
