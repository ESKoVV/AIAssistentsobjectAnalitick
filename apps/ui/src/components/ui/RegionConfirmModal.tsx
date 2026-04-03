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
  if (isSelectionStep) {
    return (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 p-4">
        <div className="w-full max-w-md rounded-xl border border-slate-700 bg-panel p-5">
          <h3 className="mb-3 text-lg font-semibold">Выберите регион</h3>
          <div className="grid gap-2">
            {regions.map((region) => (
              <button
                key={region}
                onClick={() => onConfirm(region)}
                className="rounded-md border border-slate-600 px-3 py-2 text-left hover:bg-slate-800"
              >
                {region}
              </button>
            ))}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 p-4">
      <div className="w-full max-w-md rounded-xl border border-slate-700 bg-panel p-5">
        <h3 className="mb-2 text-lg font-semibold">Подтверждение региона</h3>
        <p className="mb-4 text-slate-300">Вы из региона: <span className="font-semibold">{detectedRegion}</span>?</p>
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
