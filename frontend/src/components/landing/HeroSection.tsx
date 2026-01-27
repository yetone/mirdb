/**
 * Hero Section Component.
 * Owner: Scenario 1 - Hero Section Rendering
 *
 * Requirements:
 * - Full-width section with BackgroundEffect
 * - Large, bold headline with value proposition
 * - Supporting subheadline
 * - Primary CTA button (Get Started/Register) using FuturisticButton
 * - Secondary CTA for login
 *
 * Expected exports:
 * - HeroSection: React.FC
 */

import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import { BackgroundEffect } from '../BackgroundEffect';
import { FuturisticButton } from '../FuturisticButton';

export function HeroSection() {
  return (
    <section
      className="relative min-h-screen flex items-center justify-center overflow-hidden"
      aria-label="Hero section"
    >
      {/* Background Effect */}
      <BackgroundEffect />

      {/* Content */}
      <div className="relative z-10 container mx-auto px-4 text-center">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
        >
          {/* Headline */}
          <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 text-base-content">
            Shorten, Share, and Track Your Links
          </h1>

          {/* Subheadline */}
          <p className="text-lg md:text-xl lg:text-2xl text-base-content/70 mb-10 max-w-3xl mx-auto">
            Create short, memorable links in seconds. Gain powerful insights with
            detailed analytics to understand your audience better.
          </p>

          {/* CTA Buttons */}
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <Link to="/register">
              <FuturisticButton variant="primary" size="lg">
                Get Started
              </FuturisticButton>
            </Link>
            <Link to="/login">
              <FuturisticButton variant="secondary" size="lg">
                Login
              </FuturisticButton>
            </Link>
          </div>
        </motion.div>
      </div>
    </section>
  );
}

export default HeroSection;
