import { createContext, useContext, useState, useEffect, type ReactNode } from "react";
import type { AutomationStatus } from "../types";

export type AutomationSettingsState = {
  autoArea: string;
  setAutoArea: (v: string) => void;
  autoIntervalSeconds: string;
  setAutoIntervalSeconds: (v: string) => void;
  autoSyncLimit: string;
  setAutoSyncLimit: (v: string) => void;
  autoRdapLimit: string;
  setAutoRdapLimit: (v: string) => void;
  autoBusinessScoreLimit: string;
  setAutoBusinessScoreLimit: (v: string) => void;
  dailyTargetEnabled: boolean;
  setDailyTargetEnabled: (v: boolean) => void;
  dailyTargetAllowRecycle: boolean;
  setDailyTargetAllowRecycle: (v: boolean) => void;
  dailyTargetCount: string;
  setDailyTargetCount: (v: string) => void;
  dailyTargetMinScore: string;
  setDailyTargetMinScore: (v: string) => void;
};

const AutomationSettingsContext = createContext<AutomationSettingsState | null>(null);

export function AutomationSettingsProvider({
  automation,
  children,
}: {
  automation: AutomationStatus | null;
  children: ReactNode;
}) {
  const [autoArea, setAutoArea] = useState("");
  const [autoIntervalSeconds, setAutoIntervalSeconds] = useState("900");
  const [autoSyncLimit, setAutoSyncLimit] = useState("2000");
  const [autoRdapLimit, setAutoRdapLimit] = useState("50");
  const [autoBusinessScoreLimit, setAutoBusinessScoreLimit] = useState("500");
  const [dailyTargetEnabled, setDailyTargetEnabled] = useState(true);
  const [dailyTargetAllowRecycle, setDailyTargetAllowRecycle] = useState(true);
  const [dailyTargetCount, setDailyTargetCount] = useState("100");
  const [dailyTargetMinScore, setDailyTargetMinScore] = useState("40");

  useEffect(() => {
    if (automation) {
      setAutoArea(automation.settings.area ? String(automation.settings.area) : "");
      setAutoIntervalSeconds(String(automation.settings.interval_seconds ?? 900));
      setAutoSyncLimit(String(automation.settings.sync_limit ?? 100));
      setAutoRdapLimit(String(automation.settings.rdap_limit ?? 5));
      setAutoBusinessScoreLimit(String(automation.settings.business_score_limit ?? 500));
      setDailyTargetEnabled(Boolean(automation.settings.daily_target_enabled));
      setDailyTargetAllowRecycle(Boolean(automation.settings.daily_target_allow_recycle ?? true));
      setDailyTargetCount(String(automation.settings.daily_target_count ?? 100));
      setDailyTargetMinScore(String(automation.settings.daily_target_min_score ?? 40));
    }
  }, [automation]);

  return (
    <AutomationSettingsContext.Provider value={{
      autoArea, setAutoArea,
      autoIntervalSeconds, setAutoIntervalSeconds,
      autoSyncLimit, setAutoSyncLimit,
      autoRdapLimit, setAutoRdapLimit,
      autoBusinessScoreLimit, setAutoBusinessScoreLimit,
      dailyTargetEnabled, setDailyTargetEnabled,
      dailyTargetAllowRecycle, setDailyTargetAllowRecycle,
      dailyTargetCount, setDailyTargetCount,
      dailyTargetMinScore, setDailyTargetMinScore,
    }}>
      {children}
    </AutomationSettingsContext.Provider>
  );
}

export function useAutomationSettings(): AutomationSettingsState {
  const ctx = useContext(AutomationSettingsContext);
  if (!ctx) throw new Error("useAutomationSettings must be used within AutomationSettingsProvider");
  return ctx;
}
