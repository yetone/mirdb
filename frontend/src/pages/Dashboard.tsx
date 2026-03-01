import Navbar from '../components/Navbar'
import BackgroundEffect from '../components/BackgroundEffect'

export default function Dashboard() {
  return (
    <BackgroundEffect>
      <Navbar />
      <main className="container mx-auto px-4 py-16">
        <h1 className="text-3xl font-bold mb-8">Dashboard</h1>
        <p className="text-base-content/70">Welcome to your dashboard. Manage your shortened URLs here.</p>
      </main>
    </BackgroundEffect>
  )
}
