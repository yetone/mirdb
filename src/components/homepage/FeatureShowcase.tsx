import React from 'react';
import { GlassMorphismCard } from '../common';

/**
 * FeatureShowcase component displaying key product features.
 * Owner: Scenario 1 - Value Proposition Discovery
 *
 * Contains 3 feature cards highlighting:
 * - Instant URL shortening
 * - Click analytics and tracking
 * - Share tokens for team collaboration
 */
export const FeatureShowcase = () => {
  const features = [
    {
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" className="h-12 w-12" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
        </svg>
      ),
      title: 'Instant URL Shortening',
      description: 'Transform lengthy URLs into concise, shareable links in seconds with one click.'
    },
    {
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" className="h-12 w-12" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm7-7h-2a2 2 0 00-2 2v7a2 2 0 002 2h2a2 2 0 002-2v-7a2 2 0 00-2-2zm3-4a2 2 0 00-2-2H9a2 2 0 00-2 2v4a2 2 0 002 2h7a2 2 0 002-2V9z" />
        </svg>
      ),
      title: 'Real-time Analytics',
      description: 'Track clicks, geographic data, and device information for every shortened link.'
    },
    {
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" className="h-12 w-12" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m-.356-12a3 3 0 010 2.714M5.644 4.356a3 3 0 00-.356-1.857v2.714a3 3 0 00.356 1.857M9 14v-2a3 3 0 013-3h.01a3 3 0 012.99 3v2" />
        </svg>
      ),
      title: 'Team Collaboration',
      description: 'Share analytics with share tokens to collaborate with your team members securely.'
    }
  ];

  return (
    <section className="py-20 px-4 max-w-6xl mx-auto">
      <div className="text-center mb-16">
        <h2 className="text-4xl font-bold mb-4">Powerful Features</h2>
        <p className="text-xl max-w-3xl mx-auto">
          Everything you need to manage and track your shortened links
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
        {features.map((feature, index) => (
          <GlassMorphismCard key={index} className="p-8">
            <div className="flex flex-col items-center text-center">
              {feature.icon}
              <h3 className="text-2xl font-bold mt-6 mb-4">{feature.title}</h3>
              <p className="text-base opacity-90">{feature.description}</p>
            </div>
          </GlassMorphismCard>
        ))}
      </div>
    </section>
  );
};