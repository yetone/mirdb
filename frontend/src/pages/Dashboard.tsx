/**
 * Dashboard Page Component
 * Placeholder for existing dashboard functionality
 */
import React from 'react';
import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';

export function Dashboard() {
  return (
    <div className="min-h-screen bg-base-100" data-testid="dashboard-page">
      <BackgroundEffect />
      <Navbar />
      <main className="pt-24 px-4">
        <div className="max-w-6xl mx-auto">
          <h1 className="text-3xl font-bold mb-6">Dashboard</h1>
          <p className="text-base-content/70">Your shortened URLs will appear here.</p>
        </div>
      </main>
    </div>
  );
}
