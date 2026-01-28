import { useState } from 'react';
import { GlassMorphismCard } from '../components/GlassMorphismCard';
import { FuturisticButton } from '../components/FuturisticButton';
import { Link2, Copy, ExternalLink, BarChart2 } from 'lucide-react';
import { Link } from 'react-router-dom';

const Dashboard = () => {
  const [newUrl, setNewUrl] = useState('');

  const handleShorten = (e: React.FormEvent) => {
    e.preventDefault();
    // Placeholder - will be implemented with actual API integration
    console.log('Shorten URL:', newUrl);
    setNewUrl('');
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <h1 className="text-3xl font-bold mb-8">Dashboard</h1>

      {/* URL Shortener Form */}
      <GlassMorphismCard className="p-6 mb-8">
        <h2 className="text-xl font-semibold mb-4">Shorten a URL</h2>
        <form onSubmit={handleShorten} className="flex gap-4">
          <input
            type="url"
            placeholder="Enter your long URL here..."
            className="input input-bordered flex-1"
            value={newUrl}
            onChange={(e) => setNewUrl(e.target.value)}
            required
          />
          <FuturisticButton type="submit" variant="primary">
            <Link2 className="h-4 w-4 mr-2" />
            Shorten
          </FuturisticButton>
        </form>
      </GlassMorphismCard>

      {/* URLs List - Placeholder */}
      <GlassMorphismCard className="p-6">
        <h2 className="text-xl font-semibold mb-4">Your URLs</h2>
        <div className="overflow-x-auto">
          <table className="table w-full">
            <thead>
              <tr>
                <th>Short URL</th>
                <th>Original URL</th>
                <th>Clicks</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {/* Placeholder rows */}
              <tr>
                <td colSpan={4} className="text-center text-base-content/50 py-8">
                  No URLs yet. Create your first short URL above!
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        {/* Example row for reference */}
        <div className="hidden">
          <tr>
            <td>
              <code className="text-primary">abc123</code>
            </td>
            <td className="max-w-xs truncate">https://example.com/very-long-url</td>
            <td>42</td>
            <td className="flex gap-2">
              <button className="btn btn-ghost btn-sm">
                <Copy className="h-4 w-4" />
              </button>
              <button className="btn btn-ghost btn-sm">
                <ExternalLink className="h-4 w-4" />
              </button>
              <Link to="/stats/abc123" className="btn btn-ghost btn-sm">
                <BarChart2 className="h-4 w-4" />
              </Link>
            </td>
          </tr>
        </div>
      </GlassMorphismCard>
    </div>
  );
};

export default Dashboard;
