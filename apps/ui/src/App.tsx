import { useQueryClient } from '@tanstack/react-query';
import { useEffect, useMemo, useState } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import { Header } from './components/layout/Header';
import { PageWrapper } from './components/layout/PageWrapper';
import { Sidebar } from './components/layout/Sidebar';
import { RegionConfirmModal } from './components/ui/RegionConfirmModal';
import { Analytics } from './pages/Analytics';
import { Dashboard } from './pages/Dashboard';
import { DocumentDetail } from './pages/DocumentDetail';
import { Feed } from './pages/Feed';
import { Topics } from './pages/Topics';
import { getClosestRegion, REGION_POINTS } from './utils/regions';

function App() {
  const queryClient = useQueryClient();
  const [detectedRegion, setDetectedRegion] = useState<string>('Москва');
  const [showConfirm, setShowConfirm] = useState(false);
  const [showSelection, setShowSelection] = useState(false);

  const regions = useMemo(() => REGION_POINTS.map((r) => r.name), []);

  useEffect(() => {
    const alreadySelected = window.localStorage.getItem('selectedRegion');
    const isKnownRegion = alreadySelected ? REGION_POINTS.some((region) => region.name === alreadySelected) : false;
    const fallbackRegion = isKnownRegion ? (alreadySelected as string) : REGION_POINTS[0].name;

    if (!navigator.geolocation) {
      setDetectedRegion(fallbackRegion);
      setShowConfirm(true);
      return;
    }

    navigator.geolocation.getCurrentPosition(
      (position) => {
        const region = getClosestRegion(position.coords.latitude, position.coords.longitude);
        setDetectedRegion(region);
        setShowConfirm(true);
      },
      () => {
        setDetectedRegion(fallbackRegion);
        setShowConfirm(true);
      },
      { timeout: 4000 }
    );
  }, []);

  const handleRegionConfirm = (region: string) => {
    window.localStorage.setItem('selectedRegion', region);
    setShowConfirm(false);
    setShowSelection(false);
  };

  return (
    <div className="min-h-screen flex flex-col">
      <Header onRefresh={() => queryClient.invalidateQueries()} />
      <div className="flex flex-1">
        <Sidebar />
        <main className="flex-1">
          <PageWrapper>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/feed" element={<Feed />} />
              <Route path="/topics" element={<Topics />} />
              <Route path="/document/:id" element={<DocumentDetail />} />
              <Route path="/analytics" element={<Analytics />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </PageWrapper>
        </main>
      </div>

      {(showConfirm || showSelection) && (
        <RegionConfirmModal
          detectedRegion={detectedRegion}
          regions={regions}
          isSelectionStep={showSelection}
          onConfirm={handleRegionConfirm}
          onReject={() => {
            setShowConfirm(false);
            setShowSelection(true);
          }}
        />
      )}
    </div>
  );
}

export default App;
