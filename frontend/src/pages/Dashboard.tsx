import React from 'react';
import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';
import { GlassMorphismCard } from '../components/GlassMorphismCard';

export function Dashboard() {
  return (
    <div className="min-h-screen">
      <BackgroundEffect />
      <Navbar />
      <main className="pt-24 px-4">
        <div className="max-w-6xl mx-auto">
          <h1 className="text-3xl font-bold mb-8">Dashboard</h1>
          <GlassMorphismCard>
            <p className="text-lg text-base-content/70">
              Welcome to your dashboard. Start shortening URLs!
            </p>
          </GlassMorphismCard>
        </div>
      </main>
    </div>
  );
}
