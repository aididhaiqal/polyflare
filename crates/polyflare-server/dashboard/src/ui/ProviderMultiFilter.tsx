import * as Popover from "@radix-ui/react-popover";

import { providerSelectionLabel } from "../lib/providerSelection";
import { Check, ChevronDown } from "./icons";

export interface ProviderFilterOption {
  value: string;
  label: string;
}

export function ProviderMultiFilter({
  options,
  selected,
  modelProviders,
  onChange,
}: {
  options: ProviderFilterOption[];
  selected: string[];
  modelProviders: string[];
  onChange: (selected: string[]) => void;
}) {
  const selectedSet = new Set(selected);
  const allProviders = options.map((option) => option.value);
  const summary =
    selected.length === 1
      ? options.find((option) => option.value === selected[0])?.label ?? selected[0]
      : providerSelectionLabel(selected, modelProviders, allProviders);

  return (
    <Popover.Root>
      <Popover.Trigger asChild>
        <button
          type="button"
          className="flex shrink-0 items-center gap-1.5 rounded border border-border bg-card px-2.5 py-1 text-[10.5px] text-fg opacity-80 outline-none hover:opacity-100 focus:opacity-100"
        >
          <span className="opacity-60">Providers:</span>
          <span>{summary}</span>
          <ChevronDown className="h-3 w-3" strokeWidth={2} />
        </button>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content
          align="start"
          sideOffset={4}
          className="z-50 min-w-[210px] rounded border border-border bg-card p-1 text-[10.5px] text-fg shadow-lg"
        >
          <div className="flex gap-1 border-b border-border/70 p-1 pb-2">
            <button
              type="button"
              onClick={() => onChange(modelProviders)}
              className="rounded bg-muted px-2 py-1 font-semibold opacity-75 hover:opacity-100"
            >
              Models
            </button>
            <button
              type="button"
              onClick={() => onChange(allProviders)}
              className="rounded bg-muted px-2 py-1 font-semibold opacity-75 hover:opacity-100"
            >
              All
            </button>
          </div>
          <div className="max-h-64 overflow-auto p-1">
            {options.map((option) => {
              const checked = selectedSet.has(option.value);
              return (
                <button
                  key={option.value}
                  type="button"
                  role="checkbox"
                  aria-checked={checked}
                  onClick={() =>
                    onChange(
                      checked
                        ? selected.filter((provider) => provider !== option.value)
                        : [...selected, option.value],
                    )
                  }
                  className="flex w-full items-center gap-2 rounded px-2 py-1.5 text-left opacity-80 hover:bg-muted hover:opacity-100"
                >
                  <span className="flex h-3.5 w-3.5 items-center justify-center rounded border border-border">
                    {checked && <Check className="h-3 w-3 text-accent" strokeWidth={2.5} />}
                  </span>
                  <span>{option.label}</span>
                </button>
              );
            })}
          </div>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
