import clsx from "clsx";

export function TransportPill({
  transport,
  className,
}: {
  transport: string | null;
  className?: string;
}) {
  if (!transport) return <span className={clsx("text-fg opacity-40", className)}>—</span>;

  const normalized = transport.toLowerCase();
  return (
    <span
      title={`${normalized.toUpperCase()} transport`}
      className={clsx(
        "inline-block whitespace-nowrap rounded px-1.5 py-0.5 text-[9px] font-bold uppercase leading-none",
        normalized === "ws"
          ? "bg-accent/15 text-accent"
          : normalized === "sse"
            ? "bg-success/15 text-success"
            : "bg-muted text-fg opacity-70",
        className,
      )}
    >
      {normalized}
    </span>
  );
}
