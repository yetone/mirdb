import React from 'react'
import HowItWorksSection from '../components/homepage/HowItWorksSection'

const Home: React.FC = () => {
  return (
    <main className="min-h-screen bg-base-100">
      <section className="hero min-h-[60vh] bg-base-200">
        <div className="hero-content text-center">
          <div className="max-w-md">
            <h1 className="text-5xl font-bold">URL Shortener</h1>
            <p className="py-6">
              Shorten your URLs, track clicks, and analyze performance with our powerful link management platform.
            </p>
            <button className="btn btn-primary">Get Started</button>
          </div>
        </div>
      </section>

      <section className="py-16 px-4 bg-base-100">
        <div className="container mx-auto">
          <h2 className="text-3xl font-bold text-center mb-12">Features</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="card bg-base-200 shadow-xl">
              <div className="card-body">
                <h3 className="card-title">URL Shortening</h3>
                <p>Transform long URLs into short, memorable links instantly.</p>
              </div>
            </div>
            <div className="card bg-base-200 shadow-xl">
              <div className="card-body">
                <h3 className="card-title">Click Analytics</h3>
                <p>Track every click with detailed analytics and insights.</p>
              </div>
            </div>
            <div className="card bg-base-200 shadow-xl">
              <div className="card-body">
                <h3 className="card-title">Real-time Tracking</h3>
                <p>Monitor your links in real-time with live updates.</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      <HowItWorksSection />

      <footer className="footer footer-center p-10 bg-base-200 text-base-content">
        <div>
          <p>Copyright 2024 - URL Shortener</p>
        </div>
      </footer>
    </main>
  )
}

export default Home
