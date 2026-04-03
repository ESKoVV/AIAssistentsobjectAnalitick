import { useMemo, useState } from 'react';

interface RegionConfirmModalProps {
  detectedRegion: string;
  regions: string[];
  isSelectionStep: boolean;
  onConfirm: (region: string) => void;
  onReject: () => void;
}

export const RegionConfirmModal = ({
  detectedRegion,
  regions,
  isSelectionStep,
  onConfirm,
  onReject
}: RegionConfirmModalProps) => {
  const [manualRegion, setManualRegion] = useState('');
  const sortedRegions = useMemo(() => [...regions].sort((a, b) => a.localeCompare(b, 'ru')), [regions]);

  if (isSelectionStep) {
    return (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 p-4 backdrop-blur-sm">
        <div className="w-full max-w-md rounded-xl border border-slate-700 bg-panel p-5 shadow-2xl shadow-slate-950/40">
          <h3 className="mb-1 text-lg font-semibold">Выбор региона</h3>
          <p className="mb-4 text-sm text-slate-300">Укажите ваш регион, чтобы применить его в фильтрах по умолчанию.</p>

          <select
            value={manualRegion}
            onChange={(e) => setManualRegion(e.target.value)}
            className="mb-4 w-full rounded-md border border-slate-600 bg-slate-900 px-3 py-2"
          >
            <option value="">Выберите регион…</option>
            {sortedRegions.map((region) => (
              <option key={region} value={region}>
                {region}
              </option>
            ))}
          </select>

          <div className="flex gap-2">
            <button
              onClick={() => manualRegion && onConfirm(manualRegion)}
              disabled={!manualRegion}
              className="rounded-md bg-blue-600 px-4 py-2 disabled:cursor-not-allowed disabled:opacity-50"
            >
              Подтвердить
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 p-4 backdrop-blur-sm">
      <div className="w-full max-w-md rounded-xl border border-slate-700 bg-panel p-5 shadow-2xl shadow-slate-950/40">
        <h3 className="mb-2 text-lg font-semibold">Подтверждение региона</h3>
        <p className="mb-4 text-slate-300">
          Вы из <span className="font-semibold text-slate-100">{detectedRegion}</span> региона?
        </p>
        <div className="flex gap-2">
          <button onClick={() => onConfirm(detectedRegion)} className="rounded-md bg-blue-600 px-4 py-2">
            Да
          </button>
          <button onClick={onReject} className="rounded-md border border-slate-600 px-4 py-2 hover:bg-slate-800">
            Нет
          </button>
        </div>
      </div>
    </div>
  );
};
