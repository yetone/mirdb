/**
 * MirDB Homepage - Main Entry Point
 */
import Head from 'next/head';
import { Hero } from '@/components/sections/Hero';
import { Navbar } from '@/components/layout/Navbar';
import { Footer } from '@/components/layout/Footer';
import { Features } from '@/components/sections/Features';
import { QuickStart } from '@/components/sections/QuickStart';
import { Performance } from '@/components/sections/Performance';
import { Comparison } from '@/components/sections/Comparison';
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
      <Navbar />
      <main style={{ paddingTop: 'var(--navbar-height)' }}>
        {/* Hero section - Scenario 1 */}
        <Hero />

        {/* Features section - Scenario 3 */}
        <Features />

        {/* Quick Start section - Scenario 4 */}
        <QuickStart />

        {/* Performance section - Scenario 5 */}
        <Performance />

        {/* Comparison section - Scenario 6 */}
        <Comparison />
      </main>
      {/* Footer section - Scenario 7 */}
      <Footer />
    </>
  );
}
