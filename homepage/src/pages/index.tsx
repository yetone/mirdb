/**
 * MirDB Homepage - Main Entry Point
 * Owner: Scenario 1 - Hero Section Display (hero section portion)
 */
import Head from 'next/head';
import { Hero } from '@/components/sections/Hero';
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
        <Hero />
      </main>
    </>
  );
}
