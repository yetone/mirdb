import { Link, Globe, BarChart3, Share2 } from 'lucide-react'
import GlassMorphismCard from '../components/GlassMorphismCard'

const features = [
  {
    id: 'url-shortening',
    title: 'URL Shortening',
    description: 'Create memorable short links in seconds. Transform long URLs into clean, shareable links.',
    icon: <Link className="w-6 h-6" />,
  },
  {
    id: 'analytics-dashboard',
    title: 'Analytics Dashboard',
    description: 'Track clicks and performance with detailed analytics. See how your links are performing in real-time.',
    icon: <BarChart3 className="w-6 h-6" />,
  },
  {
    id: 'geographic-insights',
    title: 'Geographic Insights',
    description: 'Know where your audience is. Get detailed geographic data about your link visitors.',
    icon: <Globe className="w-6 h-6" />,
  },
  {
    id: 'share-collaborate',
    title: 'Share & Collaborate',
    description: 'Share public stats with your team. Collaborate and make data-driven decisions together.',
    icon: <Share2 className="w-6 h-6" />,
  },
]

export default function Home() {
  return (
    <div className="min-h-screen bg-base-100">
      {/* Hero Section */}
      <section className="hero min-h-[60vh] bg-gradient-to-br from-primary/20 to-secondary/20">
        <div className="hero-content text-center">
          <div className="max-w-2xl">
            <h1 className="text-5xl font-bold mb-6">Shorten, Share, Track</h1>
            <p className="text-xl mb-8 text-base-content/80">
              Create short, memorable links and track their performance with powerful analytics.
            </p>
            <div className="flex gap-4 justify-center">
              <button className="btn btn-primary">Get Started Free</button>
              <button className="btn btn-outline">Learn More</button>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section id="features" className="py-20 px-4" data-testid="features-section">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-3xl font-bold text-center mb-12">Key Features</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6" data-testid="features-grid">
            {features.map((feature) => (
              <GlassMorphismCard
                key={feature.id}
                data-testid={`feature-card-${feature.id}`}
                title={feature.title}
                description={feature.description}
                icon={feature.icon}
              >
                <></>
              </GlassMorphismCard>
            ))}
          </div>
        </div>
      </section>
    </div>
  )
}
