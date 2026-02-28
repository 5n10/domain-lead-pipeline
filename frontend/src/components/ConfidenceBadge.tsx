type Props = { confidence: string };

const config: Record<string, { label: string; bg: string; text: string }> = {
  high: { label: "High", bg: "bg-conf-high", text: "text-white" },
  medium: { label: "Med", bg: "bg-conf-medium", text: "text-white" },
  low: { label: "Low", bg: "bg-conf-low", text: "text-white" },
  unverified: { label: "None", bg: "bg-conf-unverified", text: "text-white" },
};

export default function ConfidenceBadge({ confidence }: Props) {
  const c = config[confidence] ?? config.unverified;
  return (
    <span
      className={`inline-block px-2 py-0.5 rounded text-xs font-semibold ${c.bg} ${c.text}`}
      title={`Verification confidence: ${confidence}`}
    >
      {c.label}
    </span>
  );
}
