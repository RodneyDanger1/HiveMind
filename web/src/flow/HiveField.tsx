/**
 * Reference board: HiveMind spatial canvas
 * (the authoritative 1440x900 spatial composition).
 *
 * Presentational backdrop for the spatial canvas: honeycomb vector field, the
 * five-lobe beehive, ambient glow and sheen. Extracted from Canvas.tsx so the
 * live canvas and replay can share one static layer. A single <svg> element, no
 * filters, no SMIL and no per-frame JS; the glow and sheen motion comes from
 * styles/motion.css (`motion-honeycomb-glow`, `motion-hive-sheen`) and every
 * colour is a CSS variable.
 */
import { useState } from 'react';
import { useAppReducedMotion } from '../state/useAppReducedMotion';

/** Random quiet interval for the honeycomb gold ripple: 12–30 seconds inclusive. */
function randomRippleSeconds(): number {
  return 12 + Math.floor(Math.random() * 19);
}

/** Figure-eight flight path around the 620x660 hive; decorative only. */
const FIGURE_EIGHT_PATH =
  'M310 330 C382 238 505 248 505 330 C505 412 382 422 310 330 C238 238 115 248 115 330 C115 412 238 422 310 330 Z';

function AmbientBee({
  scale,
  duration,
  begin,
  opacity,
}: {
  readonly scale: number;
  readonly duration: string;
  readonly begin: string;
  readonly opacity: number;
}) {
  return (
    <g
      className="ambient-bee"
      style={{ opacity, filter: 'blur(0.18px)' }}
      transform={`scale(${scale})`}
    >
      <animateMotion
        path={FIGURE_EIGHT_PATH}
        dur={duration}
        begin={begin}
        repeatCount="indefinite"
        rotate="auto"
        keyPoints="0;1;0"
        keyTimes="0;0.5;1"
        calcMode="linear"
      />
      <ellipse cx="10" cy="10" rx="7" ry="4.2" fill="var(--color-honey-300)" />
      <path d="M3 10h14M6 6.4h8M6 13.6h8" stroke="var(--color-ink-800)" strokeWidth="1.2" />
      <path
        d="M6 6.6C4 4 1.8 3.6 0.6 4.5c1.8.6 2.9 1.4 5 2.8ZM14 6.6c2-2.6 4.2-3 5.4-2.1-1.8.6-2.9 1.4-5 2.8Z"
        fill="var(--color-honey-100)"
      />
    </g>
  );
}

/** Beehive art box, unchanged from Canvas.tsx. */
const HIVE_BOX_WIDTH = 620;
const HIVE_BOX_HEIGHT = 660;

/** Lobe centre lines inside the 620x660 box; every lobe is rx 270 / ry 108. */
const LOBE_CY = [95, 215, 335, 455, 575] as const;

/** Sheen box mirrors the hive box so the highlight lands on the beehive. */
const SHEEN_BOX = 'pointer-events-none absolute h-[660px] w-[620px]';
const SHEEN_BOX_CENTRED = `${SHEEN_BOX} left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2`;
const SHEEN_BOX_ANCHORED = `${SHEEN_BOX} left-0 top-0`;

export function HiveField({ center = true }: { center?: boolean }) {
  const reduced = useAppReducedMotion();
  const [rippleSeconds, setRippleSeconds] = useState(randomRippleSeconds);
  const [rippleCycle, setRippleCycle] = useState(0);
  // The hive is placed as a 620x660 viewport at 50%/50% of the field and
  // pulled back by half its box, so no measurement or JS is involved.
  const hiveOrigin = center ? '50%' : 0;
  const hiveTransform = center
    ? `translate(-${HIVE_BOX_WIDTH / 2} -${HIVE_BOX_HEIGHT / 2})`
    : undefined;

  return (
    <div aria-hidden="true" className="pointer-events-none absolute inset-0 z-0 overflow-hidden">
      {/* Glow sits under the field so the honeycomb lines and the hive stay crisp. */}
      <div className="motion-honeycomb-glow absolute inset-0" />
      <svg className="absolute inset-0 h-full w-full" width="100%" height="100%" focusable="false">
        <defs>
          <pattern id="hivefield-honeycomb" width="48" height="84" patternUnits="userSpaceOnUse">
            <path
              d="M12 0L36 0L48 21L36 42L12 42L0 21Z"
              fill="var(--color-honey-200)"
              stroke="var(--color-honey-300)"
              strokeWidth="1.1"
              opacity="0.42"
            />
            <path
              d="M-12 42L12 42L24 63L12 84L-12 84L-24 63Z"
              fill="none"
              stroke="var(--color-honey-300)"
              strokeWidth="1.1"
              opacity="0.42"
            />
          </pattern>
          {/* A soft vertical column of honey light that travels left to right. */}
          <linearGradient id="hivefield-ripple" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="var(--color-honey-300)" stopOpacity="0" />
            <stop offset="28%" stopColor="var(--color-honey-300)" stopOpacity="0.08" />
            <stop offset="48%" stopColor="var(--color-honey-100)" stopOpacity="0.22" />
            <stop offset="62%" stopColor="var(--color-honey-200)" stopOpacity="0.14" />
            <stop offset="86%" stopColor="var(--color-honey-300)" stopOpacity="0.06" />
            <stop offset="100%" stopColor="var(--color-honey-300)" stopOpacity="0" />
          </linearGradient>
          {/* Lobe body: cream core high on the lobe, gold shoulders, honey-700 silhouette. */}
          <radialGradient id="hivefield-lobe" cx="50%" cy="34%" r="60%">
            <stop offset="0%" stopColor="var(--color-honey-100)" stopOpacity="0.98" />
            <stop offset="28%" stopColor="var(--color-honey-200)" stopOpacity="0.96" />
            <stop offset="46%" stopColor="var(--color-honey-300)" stopOpacity="0.94" />
            <stop offset="64%" stopColor="var(--color-honey-500)" stopOpacity="0.95" />
            <stop offset="82%" stopColor="var(--color-honey-600)" stopOpacity="0.96" />
            <stop offset="100%" stopColor="var(--color-honey-700)" stopOpacity="0.98" />
          </radialGradient>
          {/* Darker rim at the bottom of each lobe, where the next lobe tucks under. */}
          <linearGradient id="hivefield-rim" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--color-honey-700)" stopOpacity="0" />
            <stop offset="52%" stopColor="var(--color-honey-800)" stopOpacity="0.04" />
            <stop offset="80%" stopColor="var(--color-honey-800)" stopOpacity="0.22" />
            <stop offset="100%" stopColor="var(--color-honey-800)" stopOpacity="0.52" />
          </linearGradient>
          {/* Soft top highlight plus a tighter specular core on the crest. */}
          <radialGradient id="hivefield-highlight" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="var(--color-honey-100)" stopOpacity="0.98" />
            <stop offset="58%" stopColor="var(--color-honey-100)" stopOpacity="0.3" />
            <stop offset="100%" stopColor="var(--color-honey-200)" stopOpacity="0" />
          </radialGradient>
          <radialGradient id="hivefield-spec" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="var(--color-honey-100)" stopOpacity="0.85" />
            <stop offset="100%" stopColor="var(--color-honey-100)" stopOpacity="0" />
          </radialGradient>
          {/* Wax-cell texture stamped inside each lobe; reads as comb, not noise. */}
          <pattern id="hivefield-comb-texture" width="26" height="45" patternUnits="userSpaceOnUse">
            <path
              d="M7 0h12l6 11-6 11H7L1 11Z"
              fill="none"
              stroke="var(--color-honey-800)"
              strokeWidth="0.9"
            />
            <path
              d="M-6 22h12l6 11-6 11H-6L-12 33Z"
              fill="none"
              stroke="var(--color-honey-800)"
              strokeWidth="0.9"
            />
          </pattern>
          {/* Paper vignette: the comb field fades before it reaches the frame. */}
          <radialGradient id="hivefield-vignette" cx="50%" cy="50%" r="72%">
            <stop offset="0%" stopColor="var(--color-canvas)" stopOpacity="0" />
            <stop offset="68%" stopColor="var(--color-canvas)" stopOpacity="0" />
            <stop offset="88%" stopColor="var(--color-canvas)" stopOpacity="0.45" />
            <stop offset="100%" stopColor="var(--color-canvas)" stopOpacity="0.9" />
          </radialGradient>
          {/* Entrance tunnel: ink depth with a warm honey lip at the rim. */}
          <radialGradient id="hivefield-tunnel" cx="50%" cy="76%" r="74%">
            <stop offset="0%" stopColor="var(--color-ink-900)" />
            <stop offset="78%" stopColor="var(--color-ink-900)" />
            <stop offset="100%" stopColor="var(--color-honey-800)" />
          </radialGradient>
          {/* Landing board: honey planks catching light from above. */}
          <linearGradient id="hivefield-board" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--color-honey-500)" />
            <stop offset="55%" stopColor="var(--color-honey-600)" />
            <stop offset="100%" stopColor="var(--color-honey-700)" />
          </linearGradient>
          {/* Very subtle warm drop shadow: a gradient, never a filter. */}
          <radialGradient id="hivefield-shadow" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="var(--color-honey-800)" stopOpacity="0.18" />
            <stop offset="60%" stopColor="var(--color-honey-800)" stopOpacity="0.07" />
            <stop offset="100%" stopColor="var(--color-honey-800)" stopOpacity="0" />
          </radialGradient>
          {/* Lobe stack in the 620x660 hive space; drawn once, stamped by <use>. */}
          <symbol id="hivefield-hive" viewBox="0 0 620 660">
            {LOBE_CY.map((cy) => (
              <g key={cy}>
                <ellipse cx="310" cy={cy + 14} rx="274" ry="111" fill="url(#hivefield-shadow)" />
                <ellipse cx="310" cy={cy} rx="270" ry="108" fill="url(#hivefield-lobe)" />
                <ellipse cx="310" cy={cy} rx="264" ry="103" fill="url(#hivefield-comb-texture)" opacity="0.13" />
                <ellipse cx="310" cy={cy} rx="270" ry="108" fill="url(#hivefield-rim)" />
                <ellipse
                  cx="310"
                  cy={cy - 28}
                  rx="168"
                  ry="44"
                  fill="url(#hivefield-highlight)"
                />
                <ellipse cx="310" cy={cy - 44} rx="104" ry="24" fill="url(#hivefield-spec)" />
              </g>
            ))}
            {/* The hive entrance: a recessed arch, a lit tunnel and a planked
                landing board with three wax cells, at the base of the stack. */}
            <g className="hive-entrance">
              <ellipse cx="310" cy="618" rx="116" ry="18" fill="url(#hivefield-shadow)" />
              <path d="M236 606 Q236 532 310 532 Q384 532 384 606 Z" fill="url(#hivefield-tunnel)" />
              <path
                d="M236 606 Q236 532 310 532 Q384 532 384 606"
                fill="none"
                stroke="var(--color-honey-700)"
                strokeWidth="2.4"
                opacity="0.5"
              />
              <path d="M252 606 Q252 550 310 550 Q368 550 368 606 Z" fill="var(--color-ink-900)" opacity="0.92" />
              <path d="M264 606 Q264 566 310 566 Q356 566 356 606 Z" fill="var(--color-ink-900)" />
              <ellipse
                className="hive-entrance-glow"
                cx="310"
                cy="584"
                rx="27"
                ry="13"
                fill="var(--color-honey-300)"
                opacity="0.3"
              />
              <circle cx="310" cy="578" r="4.5" fill="var(--color-honey-200)" />
              <circle
                cx="310"
                cy="578"
                r="9"
                fill="none"
                stroke="var(--color-honey-300)"
                strokeWidth="1"
                opacity="0.35"
              />
              <path
                d="M226 606H394Q400 606 400 612V618Q400 624 394 624H226Q220 624 220 618V612Q220 606 226 606Z"
                fill="url(#hivefield-board)"
                stroke="var(--color-honey-700)"
                strokeWidth="1.2"
              />
              <path d="M236 614h148M248 619h124" stroke="var(--color-honey-700)" strokeWidth="1" opacity="0.35" />
              <polygon points="292,609 298,612.5 298,617.5 292,621 286,617.5 286,612.5" fill="var(--color-honey-300)" opacity="0.7" />
              <polygon points="310,609 316,612.5 316,617.5 310,621 304,617.5 304,612.5" fill="var(--color-honey-300)" opacity="0.85" />
              <polygon points="328,609 334,612.5 334,617.5 328,621 322,617.5 322,612.5" fill="var(--color-honey-300)" opacity="0.7" />
            </g>
          </symbol>
        </defs>

        <rect width="100%" height="100%" fill="url(#hivefield-honeycomb)" opacity="0.95" />
        <rect width="100%" height="100%" fill="url(#hivefield-vignette)" />
        {reduced ? null : (
          <rect
            key={rippleCycle}
            className="honeycomb-ripple"
            x="-30%"
            y="0"
            width="30%"
            height="100%"
            fill="url(#hivefield-ripple)"
            style={{ animationDuration: `${rippleSeconds}s` }}
            onAnimationEnd={() => {
              setRippleSeconds(randomRippleSeconds());
              setRippleCycle((cycle) => cycle + 1);
            }}
          />
        )}
        <use
          href="#hivefield-hive"
          x={hiveOrigin}
          y={hiveOrigin}
          width={HIVE_BOX_WIDTH}
          height={HIVE_BOX_HEIGHT}
          transform={hiveTransform}
        />
        {reduced ? null : (
          <g className="ambient-bee-swarm" aria-hidden="true">
            <AmbientBee scale={0.7} duration="19s" begin="-2s" opacity={0.8} />
            <AmbientBee scale={0.95} duration="23s" begin="-9s" opacity={0.65} />
            <AmbientBee scale={1.2} duration="27s" begin="-15s" opacity={0.5} />
          </g>
        )}
      </svg>
      <div className={center ? SHEEN_BOX_CENTRED : SHEEN_BOX_ANCHORED}>
        <div className="motion-hive-sheen absolute inset-0" />
      </div>
    </div>
  );
}
