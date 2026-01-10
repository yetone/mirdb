import { useEffect } from 'react'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import HowItWorksSection from '../components/HowItWorksSection'
import FooterSection from '../components/FooterSection'

// SEO Configuration
const SEO_CONFIG = {
  title: 'URL Shortener - Shorten, Share, Track Your Links',
  description:
    'Create memorable short links instantly. Track clicks, analyze performance, and manage all your links in one place with our powerful URL shortener tool.',
  ogType: 'website',
}

export default function Home() {
  useEffect(() => {
    // Set document title
    document.title = SEO_CONFIG.title

    // Create or update meta description
    let metaDescription = document.querySelector('meta[name="description"]')
    if (!metaDescription) {
      metaDescription = document.createElement('meta')
      metaDescription.setAttribute('name', 'description')
      document.head.appendChild(metaDescription)
    }
    metaDescription.setAttribute('content', SEO_CONFIG.description)

    // Create or update Open Graph meta tags
    const ogTags = [
      { property: 'og:title', content: SEO_CONFIG.title },
      { property: 'og:description', content: SEO_CONFIG.description },
      { property: 'og:type', content: SEO_CONFIG.ogType },
    ]

    ogTags.forEach(({ property, content }) => {
      let metaTag = document.querySelector(`meta[property="${property}"]`)
      if (!metaTag) {
        metaTag = document.createElement('meta')
        metaTag.setAttribute('property', property)
        document.head.appendChild(metaTag)
      }
      metaTag.setAttribute('content', content)
    })

    // Cleanup function to reset title on unmount (optional)
    return () => {
      // Keep meta tags for SEO - don't remove on unmount
    }
  }, [])

  return (
    <main>
      <HeroSection />
      <FeaturesSection />
      <HowItWorksSection />
      <FooterSection />
    </main>
  )
}
