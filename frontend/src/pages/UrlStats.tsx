import { useParams } from 'react-router-dom';
import Navbar from '../components/Navbar';
import GlassMorphismCard from '../components/GlassMorphismCard';
import BackgroundEffect from '../components/BackgroundEffect';

export default function UrlStats() {
  const { shortCode } = useParams<{ shortCode: string }>();

  return (
    <div className="min-h-screen">
      <BackgroundEffect />
      <Navbar />
      <main className="container mx-auto px-4 py-8">
        <h1 className="text-3xl font-bold mb-6">URL Statistics</h1>
        <GlassMorphismCard className="p-6">
          <p>Statistics for: {shortCode}</p>
        </GlassMorphismCard>
      </main>
    </div>
  );
}
