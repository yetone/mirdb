import Navbar from '../components/Navbar';
import GlassMorphismCard from '../components/GlassMorphismCard';
import BackgroundEffect from '../components/BackgroundEffect';

export default function Dashboard() {
  return (
    <div className="min-h-screen">
      <BackgroundEffect />
      <Navbar />
      <main className="container mx-auto px-4 py-8">
        <h1 className="text-3xl font-bold mb-6">Dashboard</h1>
        <GlassMorphismCard className="p-6">
          <p>Your shortened URLs will appear here.</p>
        </GlassMorphismCard>
      </main>
    </div>
  );
}
