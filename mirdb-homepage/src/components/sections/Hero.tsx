import React from 'react';
import { Button } from '../common/Button';
import { CratesBadge } from '../ui/Badge';
import { PROJECT_INFO, QUICKSTART_URL } from '../../utils/constants';
import './Hero.css';

/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section and Value Proposition
 *
 * Displays the main hero banner with:
 * - MirDB project name in large typography (48px+ on desktop)
 * - Tagline describing the key-value store
 * - Value proposition with LSM-tree architecture
 * - Primary CTA button ("Get Started")
 */
export const Hero: React.FC = () => {
  return (
    <section className="hero" data-testid="hero-section">
      <div className="hero__container container">
        <h1 className="hero__title" data-testid="hero-title">
          {PROJECT_INFO.name}
        </h1>
        <div className="hero__badge">
          <CratesBadge version="0.1.0" />
        </div>
        <p className="hero__tagline" data-testid="hero-tagline">
          {PROJECT_INFO.tagline}
        </p>
        <p className="hero__description" data-testid="hero-description">
          {PROJECT_INFO.description}
        </p>
        <div className="hero__actions">
          <Button
            href={QUICKSTART_URL}
            variant="primary"
            size="lg"
          >
            Get Started
          </Button>
        </div>
      </div>
    </section>
  );
};

export default Hero;
