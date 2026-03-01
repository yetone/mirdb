/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * Displays the primary value proposition of the URL shortening service.
 *
 * Exports:
 * - HeroSection: React.FC - The hero section component
 *
 * Requirements:
 * - Product name/branding
 * - Compelling tagline
 * - Sign Up CTA button (links to /register)
 * - Log In CTA button (links to /login)
 * - Uses FuturisticButton for CTAs
 */

import React from 'react'
import FuturisticButton from '../FuturisticButton'

export const HeroSection: React.FC = () => {
  return (
    <section
      className="hero-section py-20 px-4 text-center"
      data-testid="hero-section"
    >
      {/* Product Name */}
      <h1 className="text-4xl md:text-6xl font-bold mb-4" data-testid="product-name">
        <span className="text-primary">URL</span>{' '}
        <span className="text-secondary">Shortener</span>
      </h1>

      {/* Tagline / Value Proposition */}
      <p
        className="text-xl text-base-content/70 mb-8 max-w-2xl mx-auto"
        data-testid="tagline"
      >
        Shorten, track, and manage your links with ease. Create memorable URLs in seconds.
      </p>

      {/* CTA Buttons */}
      <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
        <FuturisticButton to="/register" variant="primary" data-testid="signup-button">
          Sign Up
        </FuturisticButton>
        <FuturisticButton to="/login" variant="secondary" data-testid="login-button">
          Log In
        </FuturisticButton>
      </div>
    </section>
  )
}

export default HeroSection
