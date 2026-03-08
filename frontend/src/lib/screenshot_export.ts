import maplibregl from "maplibre-gl";
import type { LegendPayload } from "@/components/map-legend";

export type ScreenshotExportState = {
  style: any;
  center: [number, number];
  zoom: number;
  bearing?: number;
  pitch?: number;
  viewportWidth?: number;
  viewportHeight?: number;
  model: string;
  run: string;
  variable: { key: string; label: string };
  fh: number;
  region?: { id: string; label: string };
  loopEnabled: boolean;
};

export type ScreenshotExportOptions = {
  width?: number;
  height?: number;
  pixelRatio?: number;
  legend?: LegendPayload | null;
  overlayLines?: string[];
};

const DEFAULT_WIDTH = 1600;
const DEFAULT_HEIGHT = 900;
const DEFAULT_PIXEL_RATIO = 2;
const MAP_SETTLE_DELAY_MS = 150;
const MAP_IDLE_TIMEOUT_MS = 15_000;
const SCREENSHOT_LOGO_SRC = "/assets/logo.png";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function loadImage(src: string): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.decoding = "async";
    image.onload = () => resolve(image);
    image.onerror = () => reject(new Error(`Failed to load image: ${src}`));
    image.src = src;
  });
}

function canvasToPngBlob(canvas: HTMLCanvasElement): Promise<Blob> {
  return new Promise((resolve, reject) => {
    canvas.toBlob(
      (blob) => {
        if (!blob) {
          reject(new Error("Failed to encode screenshot PNG."));
          return;
        }
        resolve(blob);
      },
      "image/png",
      1
    );
  });
}

function drawRoundedRect(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number
): void {
  const r = Math.max(0, Math.min(radius, width / 2, height / 2));
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + width - r, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + r);
  ctx.lineTo(x + width, y + height - r);
  ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
  ctx.lineTo(x + r, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
}

function waitForMapLoad(map: maplibregl.Map): Promise<void> {
  if (map.loaded()) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    map.once("load", () => resolve());
  });
}

function waitForMapIdle(map: maplibregl.Map): Promise<void> {
  return new Promise((resolve) => {
    let done = false;
    let timeoutId: number | null = null;

    const finish = () => {
      if (done) {
        return;
      }
      done = true;
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
      map.off("idle", onIdle);
      resolve();
    };

    const onIdle = () => finish();
    map.on("idle", onIdle);
    timeoutId = window.setTimeout(finish, MAP_IDLE_TIMEOUT_MS);

    if (map.loaded() && map.areTilesLoaded()) {
      finish();
    }
  });
}

function defaultOverlayLines(state: ScreenshotExportState, legend?: LegendPayload | null): string[] {
  const model = state.model.trim() || "Model";
  const run = state.run.trim() || "Run";
  const baseVariableLabel = state.variable.label.trim() || state.variable.key.trim() || "Variable";
  const units = legend?.units?.trim();
  const variableLabel = units && !baseVariableLabel.toLowerCase().includes(`(${units.toLowerCase()})`)
    ? `${baseVariableLabel} (${units})`
    : baseVariableLabel;
  return [`${model} • ${run} • FH ${state.fh}`, variableLabel];
}

function drawGlassCard(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number
): void {
  ctx.save();
  ctx.shadowColor = "rgba(0,0,0,0.35)";
  ctx.shadowBlur = 32;
  ctx.shadowOffsetY = 8;
  ctx.fillStyle = "rgba(0,0,0,0.38)";
  drawRoundedRect(ctx, x, y, width, height, radius);
  ctx.fill();
  ctx.restore();

  const gradient = ctx.createLinearGradient(0, y, 0, y + height);
  gradient.addColorStop(0, "rgba(255,255,255,0.08)");
  gradient.addColorStop(0.22, "rgba(255,255,255,0.03)");
  gradient.addColorStop(1, "rgba(255,255,255,0)");
  ctx.save();
  ctx.fillStyle = gradient;
  drawRoundedRect(ctx, x, y, width, height, radius);
  ctx.fill();
  ctx.strokeStyle = "rgba(255,255,255,0.10)";
  ctx.lineWidth = 1;
  drawRoundedRect(ctx, x + 0.5, y + 0.5, width - 1, height - 1, Math.max(0, radius - 0.5));
  ctx.stroke();
  ctx.strokeStyle = "rgba(255,255,255,0.04)";
  ctx.lineWidth = 1;
  drawRoundedRect(ctx, x + 1.5, y + 1.5, width - 3, height - 3, Math.max(0, radius - 1.5));
  ctx.stroke();
  ctx.restore();
}

function drawOverlay(
  ctx: CanvasRenderingContext2D,
  lines: string[],
  width: number
): void {
  const cleaned = lines.map((line) => line.trim()).filter(Boolean);
  if (cleaned.length === 0) {
    return;
  }

  const paddingX = 16;
  const paddingY = 14;
  const lineHeight = 24;
  const boxX = 18;
  const boxY = 18;
  const maxWidth = Math.max(280, width * 0.6);
  const font = "700 18px system-ui, -apple-system, Segoe UI, sans-serif";

  ctx.save();
  ctx.font = font;
  let textWidth = 0;
  for (const line of cleaned) {
    textWidth = Math.max(textWidth, ctx.measureText(line).width);
  }
  const boxWidth = Math.min(maxWidth, Math.ceil(textWidth) + paddingX * 2);
  const boxHeight = cleaned.length * lineHeight + paddingY * 2 - 4;

  drawGlassCard(ctx, boxX, boxY, boxWidth, boxHeight, 12);

  ctx.fillStyle = "rgba(255,255,255,0.96)";
  ctx.textBaseline = "top";
  ctx.font = font;
  cleaned.forEach((line, index) => {
    ctx.fillText(line, boxX + paddingX, boxY + paddingY + index * lineHeight, boxWidth - paddingX * 2);
  });
  ctx.restore();
}

type LegendEntry = LegendPayload["entries"][number];
type RadarLegendGroup = {
  label: string;
  entries: LegendEntry[];
};
type PrecipPtypeLegendRow = {
  label: string;
  min: number;
  max: number;
  colors: string[];
};

const RADAR_GROUP_LABELS = ["Rain", "Snow", "Sleet", "Freezing Rain"];
const DEFAULT_PTYPE_ORDER = ["rain", "snow", "sleet", "frzr"];

function formatLegendValue(value: number): string {
  if (Number.isInteger(value)) return value.toString();
  if (Math.abs(value) < 0.1) return value.toFixed(2);
  return value.toFixed(1);
}

function compactLegendTitle(legend: LegendPayload): string {
  const title = legend.title.trim();
  const units = legend.units?.trim();
  if (!units) {
    return title;
  }
  if (title.toLowerCase().includes(units.toLowerCase())) {
    return title;
  }
  return `${title} (${units})`;
}

function radarGroupLabelForCode(code: string, index: number): string {
  const normalized = code.toLowerCase();
  if (normalized === "rain") return "Rain";
  if (normalized === "snow") return "Snow";
  if (normalized === "sleet") return "Sleet";
  if (normalized === "frzr") return "Freezing Rain";
  return RADAR_GROUP_LABELS[index] ?? `Type ${index + 1}`;
}

function isRadarPtypeLegend(legend: LegendPayload): boolean {
  const kind = legend.kind?.toLowerCase() ?? "";
  const id = legend.id?.toLowerCase() ?? "";
  return (
    kind.includes("radar_ptype") ||
    kind.includes("radar_ptype_combo") ||
    id.includes("radar") ||
    id === "radar_ptype"
  );
}

function isPrecipPtypeLegend(legend: LegendPayload): boolean {
  const kind = legend.kind?.toLowerCase() ?? "";
  const id = legend.id?.toLowerCase() ?? "";
  return kind.includes("precip_ptype") || id === "precip_ptype";
}

function groupRadarEntries(legend: LegendPayload): RadarLegendGroup[] {
  const isZero = (value: number) => Math.abs(value) < 1e-9;

  if (legend.ptype_breaks) {
    const orderedTypes = (
      Array.isArray(legend.ptype_order) && legend.ptype_order.length > 0 ? legend.ptype_order : DEFAULT_PTYPE_ORDER
    ).filter((ptype) => legend.ptype_breaks?.[ptype]);
    const groupedByMeta: RadarLegendGroup[] = [];

    for (let index = 0; index < orderedTypes.length; index += 1) {
      const ptype = orderedTypes[index];
      const boundary = legend.ptype_breaks?.[ptype];
      if (!boundary) continue;
      const offset = Number(boundary.offset);
      const count = Number(boundary.count);
      if (!Number.isFinite(offset) || !Number.isFinite(count) || offset < 0 || count <= 0) continue;
      const slice = legend.entries.slice(offset, offset + count);
      if (slice.length === 0) continue;
      groupedByMeta.push({ label: radarGroupLabelForCode(ptype, index), entries: slice });
    }

    if (groupedByMeta.length > 0) {
      return groupedByMeta;
    }
  }

  const fallbackGroups: RadarLegendGroup[] = [];
  let current: LegendEntry[] = [];
  for (const entry of legend.entries) {
    if (isZero(entry.value)) {
      if (current.length > 0) {
        fallbackGroups.push({
          label: RADAR_GROUP_LABELS[fallbackGroups.length] ?? `Type ${fallbackGroups.length + 1}`,
          entries: current,
        });
        current = [];
      }
      continue;
    }
    current.push(entry);
  }

  if (current.length > 0) {
    fallbackGroups.push({
      label: RADAR_GROUP_LABELS[fallbackGroups.length] ?? `Type ${fallbackGroups.length + 1}`,
      entries: current,
    });
  }

  return fallbackGroups;
}

function groupPrecipPtypeRows(legend: LegendPayload): PrecipPtypeLegendRow[] {
  if (!legend.ptype_breaks) return [];
  const orderedTypes = (Array.isArray(legend.ptype_order) && legend.ptype_order.length > 0 ? legend.ptype_order : [])
    .filter((ptype) => legend.ptype_breaks?.[ptype]);
  if (orderedTypes.length === 0) return [];

  const rows: PrecipPtypeLegendRow[] = [];
  for (let index = 0; index < orderedTypes.length; index += 1) {
    const ptype = orderedTypes[index];
    const boundary = legend.ptype_breaks[ptype];
    const offset = Number(boundary.offset);
    const count = Number(boundary.count);
    if (!Number.isFinite(offset) || !Number.isFinite(count) || offset < 0 || count <= 0) continue;
    const segment = legend.entries.slice(offset, offset + count);
    if (segment.length === 0) continue;
    const colors = segment.map((entry) => entry.color).filter(Boolean);
    const min = Number(segment[0]?.value);
    const max = Number(segment[segment.length - 1]?.value);
    if (colors.length === 0 || !Number.isFinite(min) || !Number.isFinite(max)) continue;
    rows.push({ label: radarGroupLabelForCode(ptype, index), min, max, colors });
  }

  return rows;
}

function fillHorizontalGradient(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  colors: string[]
): void {
  const gradient = ctx.createLinearGradient(x, 0, x + width, 0);
  const steps = Math.max(1, colors.length - 1);
  colors.forEach((color, index) => {
    gradient.addColorStop(index / steps, color);
  });
  ctx.fillStyle = gradient;
  drawRoundedRect(ctx, x, y, width, height, 8);
  ctx.fill();
}

function drawLegendLabel(
  ctx: CanvasRenderingContext2D,
  text: string,
  x: number,
  y: number,
  align: CanvasTextAlign = "left"
): void {
  ctx.textAlign = align;
  ctx.textBaseline = "alphabetic";
  ctx.fillStyle = "rgba(255,255,255,0.95)";
  ctx.fillText(text, x, y);
}

function drawBottomLegend(
  ctx: CanvasRenderingContext2D,
  legend: LegendPayload,
  width: number,
  height: number,
  bottomPadding: number
): void {
  const outerPadding = 18;
  const isPrecip = isPrecipPtypeLegend(legend);
  const isRadar = isRadarPtypeLegend(legend);
  const bandHeight = isPrecip || isRadar ? 58 : 42;
  const bandX = outerPadding;
  const bandY = height - bottomPadding - bandHeight;
  const bandWidth = width - outerPadding * 2;
  const contentX = bandX + 14;
  const contentWidth = bandWidth - 28;
  const contentBottom = bandY + bandHeight - 10;
  const barHeight = isPrecip || isRadar ? 12 : 14;
  const barY = contentBottom - barHeight;

  ctx.save();
  drawGlassCard(ctx, bandX, bandY, bandWidth, bandHeight, 12);

  if (isPrecip) {
    const rows = groupPrecipPtypeRows(legend);
    if (rows.length > 0) {
      const gap = 10;
      const sectionWidth = (contentWidth - gap * (rows.length - 1)) / rows.length;
      rows.forEach((row, index) => {
        const x = contentX + index * (sectionWidth + gap);
        ctx.font = "700 10px system-ui, -apple-system, Segoe UI, sans-serif";
        drawLegendLabel(ctx, row.label.toUpperCase(), x, bandY + 18);
        ctx.font = "600 11px system-ui, -apple-system, Segoe UI, sans-serif";
        drawLegendLabel(ctx, `${formatLegendValue(row.min)}-${formatLegendValue(row.max)}`, x, bandY + 34);
        fillHorizontalGradient(ctx, x, barY, sectionWidth, barHeight, row.colors);
        ctx.strokeStyle = "rgba(255,255,255,0.18)";
        ctx.stroke();
      });
      ctx.restore();
      return;
    }
  }

  if (isRadar) {
    const groups = groupRadarEntries(legend);
    if (groups.length > 0) {
      const gap = 10;
      const sectionWidth = (contentWidth - gap * (groups.length - 1)) / groups.length;
      groups.forEach((group, groupIndex) => {
        const x = contentX + groupIndex * (sectionWidth + gap);
        ctx.font = "700 10px system-ui, -apple-system, Segoe UI, sans-serif";
        drawLegendLabel(ctx, group.label.toUpperCase(), x, bandY + 18);
        const values = group.entries.slice().reverse();
        const swatchCount = Math.min(4, values.length);
        const swatchGap = 5;
        const swatchWidth = (sectionWidth - swatchGap * Math.max(0, swatchCount - 1)) / Math.max(1, swatchCount);
        for (let index = 0; index < swatchCount; index += 1) {
          const entry = values[Math.round((index / Math.max(1, swatchCount - 1)) * (values.length - 1))];
          const swatchX = x + index * (swatchWidth + swatchGap);
          ctx.fillStyle = entry.color;
          drawRoundedRect(ctx, swatchX, barY, swatchWidth, barHeight, 5);
          ctx.fill();
          ctx.font = "600 10px system-ui, -apple-system, Segoe UI, sans-serif";
          drawLegendLabel(ctx, formatLegendValue(entry.value), swatchX, bandY + 34);
        }
      });
      ctx.restore();
      return;
    }
  }

  if (legend.entries.length === 0) {
    ctx.restore();
    return;
  }

  fillHorizontalGradient(ctx, contentX, barY, contentWidth, barHeight, legend.entries.map((entry) => entry.color));
  ctx.strokeStyle = "rgba(255,255,255,0.18)";
  ctx.stroke();

  const labelIndices = [0, 0.25, 0.5, 0.75, 1].map((ratio) =>
    Math.min(legend.entries.length - 1, Math.max(0, Math.round((legend.entries.length - 1) * ratio)))
  );
  const dedupedIndices = labelIndices.filter((value, index) => index === 0 || value !== labelIndices[index - 1]);
  ctx.font = "600 11px system-ui, -apple-system, Segoe UI, sans-serif";
  dedupedIndices.forEach((entryIndex, index) => {
    const entry = legend.entries[entryIndex];
    const ratio = dedupedIndices.length === 1 ? 0 : index / (dedupedIndices.length - 1);
    const labelX = contentX + ratio * contentWidth;
    const align: CanvasTextAlign = index === 0 ? "left" : index === dedupedIndices.length - 1 ? "right" : "center";
    drawLegendLabel(ctx, formatLegendValue(entry.value), labelX, bandY + 21, align);
  });
  ctx.restore();
}

async function drawLogo(ctx: CanvasRenderingContext2D, width: number): Promise<void> {
  const logo = await loadImage(SCREENSHOT_LOGO_SRC);
  const padding = 18;
  const maxWidth = 180;
  const maxHeight = 52;
  const scale = Math.min(maxWidth / logo.width, maxHeight / logo.height);
  const drawWidth = Math.max(1, Math.round(logo.width * scale));
  const drawHeight = Math.max(1, Math.round(logo.height * scale));
  const cardPaddingX = 14;
  const cardPaddingY = 10;
  const cardWidth = drawWidth + cardPaddingX * 2;
  const cardHeight = drawHeight + cardPaddingY * 2;
  const cardX = width - padding - cardWidth;
  const cardY = padding;

  drawGlassCard(ctx, cardX, cardY, cardWidth, cardHeight, 12);
  ctx.save();
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = "high";
  ctx.drawImage(logo, cardX + cardPaddingX, cardY + cardPaddingY, drawWidth, drawHeight);
  ctx.restore();
}

export async function exportViewerScreenshotPng(
  state: ScreenshotExportState,
  opts: ScreenshotExportOptions = {}
): Promise<Blob> {
  if (typeof document === "undefined" || typeof window === "undefined") {
    throw new Error("Screenshot export is only available in browser environments.");
  }

  const width = Number.isFinite(opts.width)
    ? Math.max(1, Math.round(Number(opts.width)))
    : Number.isFinite(state.viewportWidth)
      ? Math.max(1, Math.round(Number(state.viewportWidth)))
      : DEFAULT_WIDTH;
  const height = Number.isFinite(opts.height)
    ? Math.max(1, Math.round(Number(opts.height)))
    : Number.isFinite(state.viewportHeight)
      ? Math.max(1, Math.round(Number(state.viewportHeight)))
      : DEFAULT_HEIGHT;
  const pixelRatio = Number.isFinite(opts.pixelRatio)
    ? Math.max(1, Number(opts.pixelRatio))
    : DEFAULT_PIXEL_RATIO;
  const overlayLines = (opts.overlayLines ?? defaultOverlayLines(state, opts.legend)).filter(Boolean);

  const container = document.createElement("div");
  container.style.position = "fixed";
  container.style.left = "-10000px";
  container.style.top = "0";
  container.style.width = `${width}px`;
  container.style.height = `${height}px`;
  container.style.pointerEvents = "none";
  container.style.opacity = "0";

  document.body.appendChild(container);

  const map = new maplibregl.Map({
    container,
    style: state.style,
    center: state.center,
    zoom: state.zoom,
    bearing: state.bearing ?? 0,
    pitch: state.pitch ?? 0,
    interactive: false,
    attributionControl: false,
    preserveDrawingBuffer: true,
    pixelRatio,
  } as maplibregl.MapOptions);

  try {
    await waitForMapLoad(map);
    await waitForMapIdle(map);
    await sleep(MAP_SETTLE_DELAY_MS);

    const capturedMapCanvas = map.getCanvas();
    const rawCanvas = document.createElement("canvas");
    rawCanvas.width = Math.max(1, Math.round(width * pixelRatio));
    rawCanvas.height = Math.max(1, Math.round(height * pixelRatio));

    const rawCtx = rawCanvas.getContext("2d");
    if (!rawCtx) {
      throw new Error("Failed to create raw screenshot canvas context.");
    }
    rawCtx.drawImage(capturedMapCanvas, 0, 0, rawCanvas.width, rawCanvas.height);

    const outputCanvas = document.createElement("canvas");
    outputCanvas.width = width;
    outputCanvas.height = height;
    const outputCtx = outputCanvas.getContext("2d");
    if (!outputCtx) {
      throw new Error("Failed to create screenshot canvas context.");
    }

    outputCtx.imageSmoothingEnabled = true;
    outputCtx.imageSmoothingQuality = "high";
    outputCtx.drawImage(rawCanvas, 0, 0, width, height);
    drawOverlay(outputCtx, overlayLines, width);

    try {
      await drawLogo(outputCtx, width);
    } catch (error) {
      console.warn("[screenshot] Logo load failed; continuing without logo.", error);
    }

    const bottomPadding = 18;
    if (opts.legend) {
      drawBottomLegend(outputCtx, opts.legend, width, height, bottomPadding);
    }

    return canvasToPngBlob(outputCanvas);
  } finally {
    map.remove();
    container.remove();
  }
}
