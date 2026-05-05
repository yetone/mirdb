/**
 * Usage section component displaying visual demonstrations of MirDB operations.
 * Owner: Scenario 4 - Visual Demonstrations
 *
 * Expected exports:
 * - UsageSection: React component
 *
 * Requirements:
 * - Displays GIFs showing memcached protocol interactions
 * - GIFs must have descriptive alt text for accessibility
 * - Visual demonstrations must be clear and readable
 */

import React from 'react';

export const UsageSection = () => {
  return (
    <section id="usage" className="usage-section">
      <h2>Usage Demonstrations</h2>
      <div className="demonstrations">
        <img
          src="/assets/images/usage/memcached-protocol.gif"
          alt="Demonstration of memcached protocol commands in MirDB showing SET and GET operations"
          width="800"
          height="450"
        />
        <img
          src="/assets/images/usage/persistence-demo.gif"
          alt="Demonstration of MirDB's persistence capabilities showing data retention after restart"
          width="800"
          height="450"
        />
      </div>
    </section>
  );
};