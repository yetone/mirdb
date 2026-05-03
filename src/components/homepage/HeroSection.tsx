import React from 'react';
import { BackgroundEffect } from '../common';

/**
 * HeroSection component displaying the main headline, subheadline, and CTAs.
 * Uses BackgroundEffect for visual appeal and integrates with theme system.
 */
export const HeroSection = () => {
  return (
    <div className="relative h-screen flex items-center justify-center overflow-hidden">
      <BackgroundEffect />
      <div className="z-10 text-center px-4">
        <h1 className="text-5xl md:text-7xl font-bold mb-6">
          Shorten Links, Track Insights
        </h1>
        <p className="text-xl md:text-2xl mb-10 max-w-2xl">
          Transform lengthy URLs into trackable links with real-time analytics.
          Create share tokens for your team members with one click.
        </p>
        <div className="flex flex-col sm:flex-row gap-4 justify-center">
          <button className="btn btn-primary">
            Try Demo
          </button>
          <button className="btn btn-secondary">
            Get Started
          </button>
        </div>
      </div>
    </div>
  );
};