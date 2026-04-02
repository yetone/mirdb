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
import { SkipLink } from '@/components/common/SkipLink';
import { SITE_TITLE, SITE_DESCRIPTION, SITE_URL, OG_IMAGE } from '@/utils/constants';

export default function Home() {
  return (
    <>
      <Head>
        <title>{SITE_TITLE}</title>
        <meta name="description" content={SITE_DESCRIPTION} />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
        <link rel="canonical" href={SITE_URL} />

        {/* Open Graph / Social Media Meta Tags */}
        <meta property="og:type" content="website" />
        <meta property="og:url" content={SITE_URL} />
        <meta property="og:title" content={SITE_TITLE} />
        <meta property="og:description" content={SITE_DESCRIPTION} />
        <meta property="og:image" content={`${SITE_URL}${OG_IMAGE}`} />
      </Head>
      <SkipLink />
      <Navbar />
      <main id="main-content" tabIndex={-1} style={{ paddingTop: 'var(--navbar-height)' }}>
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
