/**
 * Features Section Component
 * Owner: Scenario 1 - Homepage Structure and Layout
 *
 * Grid layout showcasing key product features:
 * 1. Fast and reliable URL shortening
 * 2. Detailed click analytics
 * 3. Geo-location tracking
 * 4. Referrer and browser analysis
 * 5. Secure and private
 */

import { Zap, BarChart3, Globe, Users, Shield } from 'lucide-react';
import type { Feature } from '../../types/home';

const features: Feature[] = [
  {
    icon: <Zap className="w-10 h-10 text-primary" />,
    title: 'Lightning Fast URL Shortening',
    description:
      'Create short, memorable links in milliseconds. Our optimized infrastructure ensures instant link generation every time.',
  },
  {
    icon: <BarChart3 className="w-10 h-10 text-primary" />,
    title: 'Detailed Click Analytics',
    description:
      'Track every click with comprehensive analytics. See when, where, and how your links are being accessed.',
  },
  {
    icon: <Globe className="w-10 h-10 text-primary" />,
    title: 'Geo-Location Tracking',
    description:
      'Understand your global audience. See which countries, cities, and regions your visitors come from.',
  },
  {
    icon: <Users className="w-10 h-10 text-primary" />,
    title: 'Referrer Analysis',
    description:
      'Know exactly where your traffic originates. Track referrer sources, browsers, and devices for better insights.',
  },
  {
    icon: <Shield className="w-10 h-10 text-primary" />,
    title: 'Secure & Private',
    description:
      'Your data is protected with enterprise-grade security. We respect your privacy and never share your information.',
  },
];

export default function FeaturesSection() {
  return (
    <section className="py-20 bg-base-200" data-testid="features-section">
      <div className="container mx-auto px-4">
        <div className="text-center mb-12">
          <h2 className="text-3xl font-bold mb-4">Powerful Features</h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Everything you need to manage, track, and optimize your links in one place.
          </p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
          {features.map((feature, index) => (
            <div
              key={index}
              className="card bg-base-100 shadow-lg hover:shadow-xl transition-shadow"
              data-testid={`feature-card-${index}`}
            >
              <div className="card-body items-center text-center">
                <div className="mb-4">{feature.icon}</div>
                <h3 className="card-title text-lg">{feature.title}</h3>
                <p className="text-base-content/70">{feature.description}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
