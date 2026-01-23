/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * A prominent hero section for the homepage featuring:
 * - Main headline communicating the URL shortening service value
 * - Supporting subheadline explaining key benefits
 * - Primary CTA button ("Get Started" / "Go to Dashboard" based on auth state)
 * - Secondary CTA ("Login" for unauthenticated users)
 * - Optional BackgroundEffect integration
 *
 * Related Requirements: REQ-1, REQ-2, REQ-4, REQ-8
 */

import React from 'react';
import { motion } from 'framer-motion';
import { useAuth } from '../../contexts/AuthContext';
import FuturisticButton from '../FuturisticButton';
import BackgroundEffect from '../BackgroundEffect';

const HeroSection: React.FC = () => {
  const { isAuthenticated } = useAuth();

  return (
    <section className="relative min-h-[80vh] flex items-center justify-center overflow-hidden">
      <BackgroundEffect />
      <div className="container mx-auto px-4 text-center z-10">
        <motion.h1
          className="text-4xl md:text-6xl font-bold mb-6"
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          Shorten Links. Track Clicks. Grow Insights.
        </motion.h1>
        <motion.p
          className="text-lg md:text-xl text-base-content/70 mb-8 max-w-2xl mx-auto"
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
        >
          Transform long URLs into memorable short links and gain powerful analytics
          to understand your audience better.
        </motion.p>
        <motion.div
          className="flex flex-col sm:flex-row gap-4 justify-center"
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
        >
          {isAuthenticated ? (
            <FuturisticButton to="/dashboard" variant="primary" className="btn-lg">
              Go to Dashboard
            </FuturisticButton>
          ) : (
            <>
              <FuturisticButton to="/register" variant="primary" className="btn-lg">
                Get Started
              </FuturisticButton>
              <FuturisticButton to="/login" variant="outline" className="btn-lg">
                Login
              </FuturisticButton>
            </>
          )}
        </motion.div>
      </div>
    </section>
  );
};

export default HeroSection;
