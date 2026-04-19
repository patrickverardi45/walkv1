"use client";

import React, { useEffect, useRef, useState } from "react";

export type CurrentGps = { lat: number; lon: number; accuracy_m: number };

export type MobileWalkAddEntryPayload = {
  stationText: string;
  note: string;
  photoFile: File | null;
};

function mobileButtonStyle(background: string, color: string, borderColor: string, disabled: boolean): React.CSSProperties {
  return {
    background,
    color,
    border: "2px solid #000000",
    borderRadius: 14,
    padding: "12px 14px",
    fontWeight: 800,
    cursor: disabled ? "not-allowed" : "pointer",
    opacity: disabled ? 0.65 : 1,
    fontSize: 14,
  };
}

/** Static north-up reference; map must not rotate with device heading. */
export function MobileWalkNorthCompass() {
  return (
    <div
      style={{
        position: "absolute",
        top: 12,
        right: 12,
        zIndex: 998,
        pointerEvents: "none",
        filter: "drop-shadow(0 2px 6px rgba(0,0,0,0.45))",
      }}
      aria-hidden
    >
      <svg width={56} height={56} viewBox="0 0 56 56" fill="none">
        <circle cx={28} cy={28} r={26} fill="rgba(15,23,42,0.88)" stroke="rgba(248,250,252,0.35)" strokeWidth={1.5} />
        <path d="M28 8 L34 26 L28 22 L22 26 Z" fill="#f87171" stroke="#fecaca" strokeWidth={0.75} />
        <text x={28} y={16} textAnchor="middle" fill="#fecaca" fontSize={9} fontWeight={800} fontFamily="Inter,system-ui,sans-serif">
          N
        </text>
        <text x={28} y={48} textAnchor="middle" fill="#94a3b8" fontSize={7} fontWeight={700} fontFamily="Inter,system-ui,sans-serif">
          S
        </text>
        <text x={10} y={31} textAnchor="middle" fill="#94a3b8" fontSize={7} fontWeight={700} fontFamily="Inter,system-ui,sans-serif">
          W
        </text>
        <text x={46} y={31} textAnchor="middle" fill="#94a3b8" fontSize={7} fontWeight={700} fontFamily="Inter,system-ui,sans-serif">
          E
        </text>
      </svg>
    </div>
  );
}

type EntrySheetProps = {
  open: boolean;
  busy: boolean;
  entryCountLabel: string;
  currentGps: CurrentGps | null;
  onCancel: () => void;
  onSave: (payload: MobileWalkAddEntryPayload) => void | Promise<void>;
};

export function EntrySheet({ open, busy, entryCountLabel, currentGps, onCancel, onSave }: EntrySheetProps) {
  const [stationText, setStationText] = useState("");
  const [note, setNote] = useState("");
  const [photoFile, setPhotoFile] = useState<File | null>(null);
  const [noteExpanded, setNoteExpanded] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!open) return;
    setStationText("");
    setNote("");
    setPhotoFile(null);
    setNoteExpanded(false);
  }, [open]);

  if (!open) return null;

  return (
    <div
      style={{
        position: "absolute",
        left: 10,
        right: 10,
        top: 72,
        zIndex: 1002,
        maxWidth: 480,
        marginLeft: "auto",
        marginRight: "auto",
        borderRadius: 16,
        background: "#ffffff",
        border: "1px solid #dbe4ee",
        boxShadow: "0 18px 42px rgba(0,0,0,0.28)",
        padding: 14,
        pointerEvents: "auto",
      }}
      onPointerDown={(e) => e.stopPropagation()}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", pointerEvents: "auto" }}>
        <div style={{ fontWeight: 800, fontSize: 16 }}>Add Walk Entry</div>
        <div style={{ fontSize: 12, color: "#64748b" }}>{entryCountLabel}</div>
      </div>
      {currentGps ? (
        <div style={{ marginTop: 6, fontSize: 11, color: "#64748b" }}>
          Location from walk GPS (±{Math.round(currentGps.accuracy_m)}m)
        </div>
      ) : (
        <div style={{ marginTop: 6, fontSize: 11, color: "#b45309" }}>No accepted GPS fix yet — save will try a one-time fix if needed.</div>
      )}

      <div style={{ marginTop: 12, display: "grid", gap: 12, pointerEvents: "auto" }}>
        <label style={{ display: "grid", gap: 6 }}>
          <span style={{ fontSize: 12, fontWeight: 800, color: "#475569" }}>Station</span>
          <input
            value={stationText}
            onChange={(e) => setStationText(e.target.value)}
            placeholder="12+34"
            inputMode="numeric"
            pattern="[0-9+]*"
            autoFocus
            autoComplete="off"
            style={{
              borderRadius: 12,
              border: "2px solid #0f172a",
              padding: "14px 14px",
              fontSize: 22,
              fontWeight: 800,
              width: "100%",
              boxSizing: "border-box",
            }}
          />
        </label>

        <label style={{ display: "grid", gap: 6 }}>
          <span style={{ fontSize: 12, fontWeight: 800, color: "#475569" }}>Note</span>
          <textarea
            value={note}
            onChange={(e) => setNote(e.target.value)}
            placeholder="Optional note"
            rows={noteExpanded ? 5 : 2}
            onFocus={() => setNoteExpanded(true)}
            style={{
              borderRadius: 12,
              border: "1px solid #cfd8e3",
              padding: "10px 12px",
              fontSize: 15,
              minHeight: noteExpanded ? 100 : 52,
              resize: "vertical",
              width: "100%",
              boxSizing: "border-box",
              lineHeight: 1.45,
            }}
          />
        </label>

        <input
          ref={fileInputRef}
          type="file"
          accept="image/*"
          capture="environment"
          style={{ display: "none" }}
          onChange={(e) => {
            const f = e.target.files?.[0] ?? null;
            setPhotoFile(f);
            try {
              e.currentTarget.value = "";
            } catch {
              /* ignore */
            }
          }}
        />

        <button
          type="button"
          onPointerDown={(e) => e.stopPropagation()}
          onClick={() => fileInputRef.current?.click()}
          style={{
            ...mobileButtonStyle("#f8fafc", "#0f172a", "#cbd5e1", false),
            width: "100%",
            minHeight: 48,
            fontSize: 15,
          }}
        >
          {photoFile ? `Photo: ${photoFile.name}` : "Add photo (optional)"}
        </button>

        <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 4 }}>
          <button
            type="button"
            disabled={busy}
            onClick={() => onSave({ stationText, note, photoFile })}
            style={{
              ...mobileButtonStyle("#0f172a", "#ffffff", "#000000", busy),
              width: "100%",
              minHeight: 52,
              fontSize: 17,
            }}
          >
            Save Entry
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={onCancel}
            style={{
              ...mobileButtonStyle("#ffffff", "#64748b", "#94a3b8", busy),
              width: "100%",
              minHeight: 46,
              fontSize: 15,
            }}
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

type MobileWalkUIProps = {
  busy: boolean;
  activeSession: { status: "active" | "ended"; entry_count: number } | null;
  showAddEntryModal: boolean;
  currentGps: CurrentGps | null;
  onStartWalk: () => void;
  onEndWalk: () => void;
  onOpenAddEntry: () => void;
  onCloseAddEntryModal: () => void;
  onAddEntry: (payload: MobileWalkAddEntryPayload) => void | Promise<void>;
  onSendHome: () => void;
};

export default function MobileWalkUI({
  busy,
  activeSession,
  showAddEntryModal,
  currentGps,
  onStartWalk,
  onEndWalk,
  onOpenAddEntry,
  onCloseAddEntryModal,
  onAddEntry,
  onSendHome,
}: MobileWalkUIProps) {
  return (
    <>
      <EntrySheet
        open={showAddEntryModal}
        busy={busy}
        entryCountLabel={activeSession ? `${activeSession.entry_count} entries` : "No active session"}
        currentGps={currentGps}
        onCancel={onCloseAddEntryModal}
        onSave={onAddEntry}
      />

      <div
        style={{
          position: "absolute",
          left: 10,
          right: 10,
          bottom: 10,
          zIndex: 1001,
          display: "flex",
          flexDirection: "column",
          gap: 10,
          maxWidth: 520,
          marginLeft: "auto",
          marginRight: "auto",
          pointerEvents: "auto",
        }}
        onPointerDown={(e) => e.stopPropagation()}
      >
        <button
          onPointerDown={(e) => e.stopPropagation()}
          onClick={onStartWalk}
          disabled={busy || (!!activeSession && activeSession.status === "active")}
          style={{
            width: "100%",
            ...mobileButtonStyle("#0f172a", "#ffffff", "#0f172a", busy || (!!activeSession && activeSession.status === "active")),
            fontSize: 17,
            minHeight: 54,
            paddingTop: 12,
            paddingBottom: 12,
          }}
        >
          Start Walk
        </button>
        <button
          onPointerDown={(e) => e.stopPropagation()}
          onClick={onOpenAddEntry}
          disabled={busy || !activeSession || activeSession.status !== "active"}
          style={{
            width: "100%",
            ...mobileButtonStyle("#ffffff", "#0f172a", "#cfd8e3", busy || !activeSession || activeSession.status !== "active"),
            fontSize: 17,
            minHeight: 54,
            paddingTop: 12,
            paddingBottom: 12,
          }}
        >
          Add Station / Event
        </button>
        <div style={{ display: "flex", gap: 10, width: "100%" }}>
          <button
            onPointerDown={(e) => e.stopPropagation()}
            onClick={onEndWalk}
            disabled={busy || !activeSession || activeSession.status !== "active"}
            style={{
              flex: 1,
              ...mobileButtonStyle("#ef4444", "#ffffff", "#ef4444", busy || !activeSession || activeSession.status !== "active"),
              fontSize: 17,
              minHeight: 54,
              paddingTop: 12,
              paddingBottom: 12,
            }}
          >
            End Walk
          </button>
          <button
            onPointerDown={(e) => e.stopPropagation()}
            onClick={onSendHome}
            disabled={busy}
            style={{
              flex: 1,
              ...mobileButtonStyle("#10b981", "#ffffff", "#10b981", busy),
              fontSize: 17,
              minHeight: 54,
              paddingTop: 12,
              paddingBottom: 12,
            }}
          >
            Send Home
          </button>
        </div>
      </div>
    </>
  );
}
