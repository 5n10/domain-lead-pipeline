type Props = {
  label: string;
  value: string | number;
  sub?: string;
  accent?: boolean;
};

export default function MetricCard({ label, value, sub, accent }: Props) {
  return (
    <div className={`rounded-xl border border-border p-4 ${accent ? "bg-amber-50" : "bg-bg-card"}`}>
      <p className="text-xs text-text-secondary mb-1 truncate">{label}</p>
      <p className="text-2xl font-bold tabular-nums tracking-tight">
        {typeof value === "number" ? value.toLocaleString() : value}
      </p>
      {sub && <p className="text-xs text-text-secondary mt-1">{sub}</p>}
    </div>
  );
}
