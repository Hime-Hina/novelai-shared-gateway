export const AMOUNT_SCALE = 1_000_000_000_000n;
export const AMOUNT_DECIMALS = 12;
export type AmountText = string;
export type AmountInput = string | number;
export type AmountRounding = 'exact' | 'floor' | 'ceil' | 'nearest';
export type ResourceAmountsText = Record<'v5' | 'anlasFixed' | 'anlasPurchased', AmountText>;

export function decimalRatio(value: AmountInput): { numerator: bigint; denominator: bigint } {
  if (typeof value === 'number' && !Number.isFinite(value))
    throw new RangeError('Resource amount must be finite');
  const match = /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(String(value));
  if (!match) throw new TypeError('Invalid decimal resource amount');
  const exponent = Number(match[4] || 0) - (match[3]?.length || 0);
  if (!Number.isSafeInteger(exponent) || Math.abs(exponent) > 1000)
    throw new RangeError('Resource amount exponent is too large');
  const coefficient = BigInt(`${match[1]}${match[2]}${match[3] || ''}`);
  return exponent >= 0
    ? { numerator: coefficient * 10n ** BigInt(exponent), denominator: 1n }
    : { numerator: coefficient, denominator: 10n ** BigInt(-exponent) };
}

export function divideAmount(
  numerator: bigint,
  denominator: bigint,
  rounding: AmountRounding = 'floor',
): bigint {
  if (denominator <= 0n) throw new RangeError('Amount divisor must be positive');
  const result = numerator / denominator,
    remainder = numerator % denominator;
  if (!remainder) return result;
  if (rounding === 'exact') throw new RangeError('Resource amount exceeds 12 decimal places');
  if (rounding === 'floor') return remainder < 0n ? result - 1n : result;
  if (rounding === 'ceil') return remainder > 0n ? result + 1n : result;
  return (remainder < 0n ? -remainder : remainder) * 2n >= denominator
    ? result + (remainder > 0n ? 1n : -1n)
    : result;
}

export function parseAmount(value: AmountInput, rounding: AmountRounding = 'exact'): bigint {
  const { numerator, denominator } = decimalRatio(value);
  return divideAmount(numerator * AMOUNT_SCALE, denominator, rounding);
}

export function formatAmount(units: bigint): AmountText {
  const sign = units < 0n ? '-' : '',
    absolute = units < 0n ? -units : units;
  const fraction = String(absolute % AMOUNT_SCALE)
    .padStart(AMOUNT_DECIMALS, '0')
    .replace(/0+$/, '');
  return `${sign}${absolute / AMOUNT_SCALE}${fraction ? `.${fraction}` : ''}`;
}

export const amountNumber = (units: bigint): number => Number(formatAmount(units));
export const minAmount = (...values: bigint[]): bigint =>
  values.reduce((left, right) => (left < right ? left : right));
export const maxAmount = (...values: bigint[]): bigint =>
  values.reduce((left, right) => (left > right ? left : right));

export function allocateAmount(
  total: bigint,
  weights: { id: string; weight: bigint }[],
): Record<string, bigint> {
  if (
    total < 0n ||
    weights.some((entry) => entry.weight < 0n) ||
    new Set(weights.map((entry) => entry.id)).size !== weights.length
  )
    throw new RangeError('Invalid allocation');
  const result = Object.fromEntries(weights.map((entry) => [entry.id, 0n]));
  const denominator = weights.reduce((sum, entry) => sum + entry.weight, 0n);
  if (!total || !denominator) return result;
  const remainders = weights
    .map((entry) => {
      const product = total * entry.weight;
      result[entry.id] = product / denominator;
      return { id: entry.id, remainder: product % denominator };
    })
    .sort((left, right) =>
      left.remainder === right.remainder
        ? left.id < right.id
          ? -1
          : left.id > right.id
            ? 1
            : 0
        : left.remainder > right.remainder
          ? -1
          : 1,
    );
  let remaining = total - Object.values(result).reduce((sum, value) => sum + value, 0n);
  for (const entry of remainders) {
    if (!remaining) break;
    result[entry.id] += 1n;
    remaining -= 1n;
  }
  return result;
}

function decimalFraction(numerator: bigint, denominator: bigint): string {
  const sign = numerator < 0n ? '-' : '',
    absolute = numerator < 0n ? -numerator : numerator;
  const fraction = String(absolute % denominator)
    .padStart(String(denominator).length - 1, '0')
    .replace(/0+$/, '');
  return `${sign}${absolute / denominator}${fraction ? `.${fraction}` : ''}`;
}

export function normalizeLegacyAmountGroup(entries: { id: string; value: AmountInput }[]) {
  if (new Set(entries.map((entry) => entry.id)).size !== entries.length)
    throw new Error('Duplicate opening amount identity');
  const ratios = entries.map((entry) => ({ ...entry, ...decimalRatio(entry.value) }));
  const denominator = ratios.reduce(
    (largest, entry) => (entry.denominator > largest ? entry.denominator : largest),
    1n,
  );
  const expanded = ratios.map((entry) => ({
    ...entry,
    raw: entry.numerator * (denominator / entry.denominator),
  }));
  const rawTotal = expanded.reduce((sum, entry) => sum + entry.raw, 0n),
    normalizedTotal = divideAmount(rawTotal * AMOUNT_SCALE, denominator, 'nearest');
  const amounts = Object.fromEntries(
    expanded.map((entry) => [
      entry.id,
      divideAmount(entry.raw * AMOUNT_SCALE, denominator, 'floor'),
    ]),
  );
  const remainders = expanded
    .map((entry) => ({
      id: entry.id,
      remainder: entry.raw * AMOUNT_SCALE - amounts[entry.id] * denominator,
    }))
    .sort((left, right) =>
      left.remainder === right.remainder
        ? left.id < right.id
          ? -1
          : left.id > right.id
            ? 1
            : 0
        : left.remainder > right.remainder
          ? -1
          : 1,
    );
  let remaining = normalizedTotal - Object.values(amounts).reduce((sum, value) => sum + value, 0n);
  for (const entry of remainders) {
    if (!remaining) break;
    amounts[entry.id] += 1n;
    remaining -= 1n;
  }
  const adjustments = expanded.map((entry) => {
    const normalized = amounts[entry.id];
    if (entry.raw !== 0n && (normalized === 0n || entry.raw < 0n !== normalized < 0n))
      throw new Error(
        `Opening normalization cannot preserve the sign of ${entry.id}: ${entry.value}`,
      );
    return {
      id: entry.id,
      raw: String(entry.value),
      normalized: formatAmount(normalized),
      delta: decimalFraction(
        normalized * denominator - entry.raw * AMOUNT_SCALE,
        denominator * AMOUNT_SCALE,
      ),
    };
  });
  return {
    amounts: Object.fromEntries(
      Object.entries(amounts).map(([id, amount]) => [id, formatAmount(amount)]),
    ),
    adjustments,
    rawTotal: decimalFraction(rawTotal, denominator),
    normalizedTotal: formatAmount(normalizedTotal),
    totalDelta: decimalFraction(
      normalizedTotal * denominator - rawTotal * AMOUNT_SCALE,
      denominator * AMOUNT_SCALE,
    ),
  };
}
