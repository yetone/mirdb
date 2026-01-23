/**
 * Home Page Component
 * Owner: Multiple scenarios (coordination required)
 *
 * Main landing page that composes all homepage sections
 *
 * Related Requirements: All homepage requirements
 */

import React, { useEffect } from 'react';
import Navbar from '../components/Navbar';
import { HeroSection, FeatureCards, HowItWorks, StatsSection, Footer } from '../components/homepage';

/**
 * SEO constants for the homepage
 * NFR-4: SEO optimization with proper meta tags and Open Graph support
 */
const SEO = {
  title: 'URL Shortener - Shorten Links, Track Clicks & Grow Insights',
  description: 'Create short, memorable links instantly with our powerful URL shortening service. Track every click with detailed analytics, geographic insights, and browser data.',
  ogType: 'website',
};

/**
 * Custom hook to manage SEO meta tags
 * Sets document title and Open Graph meta tags for social sharing
 */
const useSEO = (title: string, description: string, ogType: string) => {
  useEffect(() => {
    // Set document title
    document.title = title;

    // Helper to create or update meta tag
    const setMetaTag = (attribute: string, attributeValue: string, content: string) => {
      let meta = document.querySelector(`meta[${attribute}="${attributeValue}"]`) as HTMLMetaElement | null;
      if (!meta) {
        meta = document.createElement('meta');
        meta.setAttribute(attribute, attributeValue);
        document.head.appendChild(meta);
      }
      meta.setAttribute('content', content);
    };

    // Set meta description
    setMetaTag('name', 'description', description);

    // Set Open Graph tags for social sharing
    setMetaTag('property', 'og:title', title);
    setMetaTag('property', 'og:description', description);
    setMetaTag('property', 'og:type', ogType);

    // Cleanup function to restore original values when component unmounts
    return () => {
      // Clean up OG tags when navigating away
      const ogTags = ['og:title', 'og:description', 'og:type'];
      ogTags.forEach(tag => {
        const meta = document.querySelector(`meta[property="${tag}"]`);
        if (meta) {
          meta.remove();
        }
      });
    };
  }, [title, description, ogType]);
};

const Home: React.FC = () => {
  // Apply SEO meta tags for the homepage
  useSEO(SEO.title, SEO.description, SEO.ogType);

  return (
    <div className="min-h-screen flex flex-col">
      <Navbar />
      <main className="flex-1">
        <HeroSection />
        <FeatureCards />
        <HowItWorks />
        <StatsSection />
      </main>
      <Footer />
    </div>
  );
};

export default Home;
