import { createContext, useContext, useState, type ReactNode } from "react";

export type ExportState = {
  exportPlatform: string;
  setExportPlatform: (v: string) => void;
  exportMinScore: string;
  setExportMinScore: (v: string) => void;
  exportRequireContact: boolean;
  setExportRequireContact: (v: boolean) => void;
  exportRequireUnhosted: boolean;
  setExportRequireUnhosted: (v: boolean) => void;
  exportRequireDomainQualification: boolean;
  setExportRequireDomainQualification: (v: boolean) => void;
};

const ExportContext = createContext<ExportState | null>(null);

export function ExportProvider({ children }: { children: ReactNode }) {
  const [exportPlatform, setExportPlatform] = useState("csv_business");
  const [exportMinScore, setExportMinScore] = useState("40");
  const [exportRequireContact, setExportRequireContact] = useState(true);
  const [exportRequireUnhosted, setExportRequireUnhosted] = useState(false);
  const [exportRequireDomainQualification, setExportRequireDomainQualification] = useState(false);

  return (
    <ExportContext.Provider value={{
      exportPlatform, setExportPlatform,
      exportMinScore, setExportMinScore,
      exportRequireContact, setExportRequireContact,
      exportRequireUnhosted, setExportRequireUnhosted,
      exportRequireDomainQualification, setExportRequireDomainQualification,
    }}>
      {children}
    </ExportContext.Provider>
  );
}

export function useExport(): ExportState {
  const ctx = useContext(ExportContext);
  if (!ctx) throw new Error("useExport must be used within ExportProvider");
  return ctx;
}
