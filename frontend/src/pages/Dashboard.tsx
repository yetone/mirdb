import Navbar from '../components/Navbar';

export default function Dashboard() {
  return (
    <div className="min-h-screen bg-base-200">
      <Navbar />
      <div className="container mx-auto px-4 py-8">
        <h1 className="text-3xl font-bold mb-6">Dashboard</h1>
        <p className="text-base-content/70">Welcome to your dashboard. Manage your shortened URLs here.</p>
      </div>
    </div>
  );
}
