import React from 'react';

interface SocialMetric {
  value: string;
  label: string;
}

const SocialProofBar: React.FC = () => {
  const metrics: SocialMetric[] = [
    { value: '1M+', label: 'URLs shortened' },
    { value: '10M+', label: 'clicks tracked' },
    { value: '50K+', label: 'active users' }
  ];

  return (
    <div className="bg-base-200 py-4">
      <div className="container mx-auto px-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-center">
          {metrics.map((metric, index) => (
            <div key={index} className="stat">
              <div className="stat-value text-primary text-2xl">{metric.value}</div>
              <div className="stat-desc">{metric.label}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default SocialProofBar;