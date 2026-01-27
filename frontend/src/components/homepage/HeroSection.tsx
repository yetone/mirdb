/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section and Value Proposition
 *
 * Requirements: REQ-1, REQ-2
 * User Stories: US-1, US-2, US-3
 *
 * Expected functionality:
 * - Display clear value proposition headline (H1)
 * - Show supporting text with key benefits
 * - Primary CTA: "Get Started" button linking to /register
 * - Secondary CTA: "Sign In" link linking to /login
 * - Optional animated illustration/mockup
 * - Full-width container with BackgroundEffect support
 * - Framer Motion animations (respecting reduced motion)
 *
 * Props:
 * - onGetStarted?: () => void
 * - onSignIn?: () => void
 */

import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import { Link2, BarChart3, Share2 } from 'lucide-react';
import { BackgroundEffect } from '../BackgroundEffect';
import { FuturisticButton } from '../FuturisticButton';

interface HeroSectionProps {
  onGetStarted?: () => void;
  onSignIn?: () => void;
}

export function HeroSection({ onGetStarted, onSignIn }: HeroSectionProps) {
  const navigate = useNavigate();

  const handleGetStarted = () => {
    if (onGetStarted) {
      onGetStarted();
    }
    navigate('/register');
  };

  const handleSignIn = () => {
    if (onSignIn) {
      onSignIn();
    }
    navigate('/login');
  };

  return (
    <section
      data-testid="hero-section"
      className="relative min-h-screen flex items-center justify-center px-4 sm:px-6 lg:px-8"
    >
      <BackgroundEffect />

      <div className="relative z-10 max-w-4xl mx-auto text-center">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold tracking-tight mb-6">
            Shorten Your URLs,{' '}
            <span className="text-primary">Amplify Your Reach</span>
          </h1>

          <p className="text-lg sm:text-xl text-base-content/70 mb-8 max-w-2xl mx-auto">
            Create short, memorable links with powerful analytics and easy sharing.
            Track every click, understand your audience, and optimize your marketing.
          </p>

          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center mb-12">
            <FuturisticButton
              variant="primary"
              size="lg"
              onClick={handleGetStarted}
              data-testid="get-started-button"
            >
              Get Started
            </FuturisticButton>

            <FuturisticButton
              variant="secondary"
              size="lg"
              onClick={handleSignIn}
              data-testid="sign-in-button"
            >
              Sign In
            </FuturisticButton>
          </div>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 40 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.3 }}
          className="grid grid-cols-1 sm:grid-cols-3 gap-6 mt-8"
        >
          <FeatureHighlight
            icon={<Link2 className="w-6 h-6" />}
            title="Short Links"
            description="Create memorable short URLs"
          />
          <FeatureHighlight
            icon={<BarChart3 className="w-6 h-6" />}
            title="Analytics"
            description="Track clicks and performance"
          />
          <FeatureHighlight
            icon={<Share2 className="w-6 h-6" />}
            title="Easy Sharing"
            description="Share anywhere instantly"
          />
        </motion.div>
      </div>
    </section>
  );
}

interface FeatureHighlightProps {
  icon: React.ReactNode;
  title: string;
  description: string;
}

function FeatureHighlight({ icon, title, description }: FeatureHighlightProps) {
  return (
    <div className="flex flex-col items-center p-4">
      <div className="text-primary mb-2">{icon}</div>
      <h3 className="font-semibold text-base-content">{title}</h3>
      <p className="text-sm text-base-content/60">{description}</p>
    </div>
  );
}
