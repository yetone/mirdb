/**
 * MirDB Homepage - Main Entry Point
 */
import Head from 'next/head';
import { Hero } from '@/components/sections/Hero';
import { Features } from '@/components/sections/Features';
import { SITE_TITLE, SITE_DESCRIPTION } from '@/utils/constants';

export default function Home() {
  return (
    <>
      <Head>
        <title>{SITE_TITLE}</title>
        <meta name="description" content={SITE_DESCRIPTION} />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>

      <main>
        {/* Hero section - Scenario 1 */}
        <Hero />

        {/* Features section - Scenario 3 */}
        <Features />

        {/* Quick Start section - Scenario 4 */}
        <section id="quickstart">
          <div className="container">
            <h2 className="section-heading">Quick Start</h2>
            <p className="text-center">Coming soon...</p>
          </div>
        </section>

        {/* Performance section - Scenario 5 */}
        <section id="performance">
          <div className="container">
            <h2 className="section-heading">Performance</h2>
            <p className="text-center">Coming soon...</p>
          </div>
        </section>

        {/* Comparison section - Scenario 6 */}
        <section id="comparison">
          <div className="container">
            <h2 className="section-heading">Comparison</h2>
            <p className="text-center">Coming soon...</p>
          </div>
        </section>
      </main>
    </>
  );
}
