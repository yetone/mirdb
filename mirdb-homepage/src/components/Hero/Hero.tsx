import { GITHUB_REPO_URL } from '../../utils/constants'

interface HeroProps {
  logoSrc?: string
  tagline?: string
  ctaText?: string
  ctaHref?: string
}

export function Hero({
  logoSrc = '/logo.svg',
  tagline = 'A Persistent Key-Value Store with Memcached Protocol',
  ctaText = 'Get Started',
  ctaHref = GITHUB_REPO_URL,
}: HeroProps) {
  return (
    <section className="flex flex-col items-center justify-center min-h-[80vh] px-4 py-16 text-center bg-gradient-to-b from-gray-50 to-white dark:from-gray-900 dark:to-gray-800">
      <img
        src={logoSrc}
        alt="MirDB"
        width={120}
        height={120}
        className="mb-8"
        style={{ minWidth: '120px', minHeight: '120px' }}
      />
      <h1 className="text-5xl font-bold text-gray-900 dark:text-white mb-4">
        MirDB
      </h1>
      <h2 className="text-xl text-gray-600 dark:text-gray-300 mb-8 max-w-2xl">
        {tagline}
      </h2>
      <a
        href={ctaHref}
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center px-8 py-3 text-lg font-semibold text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
      >
        {ctaText}
      </a>
    </section>
  )
}
