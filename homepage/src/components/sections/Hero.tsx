/**
 * Hero Section component.
 * Owner: Scenario 1 - Hero Section Display
 *
 * Features:
 * - MirDB logo display
 * - Tagline: "Persistent Key-Value Store with Memcached Compatibility"
 * - Primary CTA: "Get Started" (scrolls to #quick-start)
 * - Secondary CTA: "View on GitHub" (external link)
 *
 * Requirements: REQ-1, REQ-2
 */

'use client';

import React from 'react';
import Image from 'next/image';
import { Button } from '@/components/ui/Button';
import { GITHUB_URL } from '@/lib/constants';

export function Hero() {
  const handleGetStarted = (e: React.MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault();
    const quickStartSection = document.getElementById('quick-start');
    if (quickStartSection) {
      quickStartSection.scrollIntoView({ behavior: 'smooth' });
    }
  };

  return (
    <section
      id="hero"
      className="py-20 px-4 text-center bg-gradient-to-b from-primary-50 to-white dark:from-slate-900 dark:to-slate-800"
      aria-labelledby="hero-heading"
    >
      <div className="max-w-4xl mx-auto">
        {/* Logo */}
        <div className="flex justify-center mb-8">
          <Image
            src="/images/logo.svg"
            alt="MirDB"
            width={120}
            height={120}
            priority
            className="w-24 h-24 md:w-32 md:h-32"
          />
        </div>

        {/* Project Name */}
        <h1
          id="hero-heading"
          className="text-4xl md:text-6xl font-bold text-primary-900 dark:text-white mb-4"
        >
          MirDB
        </h1>

        {/* Tagline */}
        <p className="text-xl md:text-2xl text-gray-600 dark:text-gray-300 max-w-2xl mx-auto mb-8">
          Persistent Key-Value Store with Memcached Compatibility
        </p>

        {/* CTA Buttons */}
        <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
          <Button
            variant="primary"
            size="lg"
            href="#quick-start"
            onClick={handleGetStarted}
          >
            Get Started
          </Button>
          <Button
            variant="secondary"
            size="lg"
            href={GITHUB_URL}
            external
          >
            View on GitHub
          </Button>
        </div>
      </div>
    </section>
  );
}
