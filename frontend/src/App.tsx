import { FeaturesSection } from './components/homepage';

function App() {
  return (
    <div className="min-h-screen bg-base-100">
      {/* Hero placeholder */}
      <section className="hero min-h-[60vh] bg-base-200">
        <div className="hero-content text-center">
          <div className="max-w-md">
            <h1 className="text-5xl font-bold">Shorten Links. Track Success.</h1>
            <p className="py-6">
              Free URL shortener with powerful analytics. Create short links, QR codes, and track every click.
            </p>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <FeaturesSection />
    </div>
  );
}

export default App;
