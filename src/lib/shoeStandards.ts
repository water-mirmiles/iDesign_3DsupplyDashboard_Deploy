/**
 * 鞋履行业常规模型长度基准（公制，mm）
 * 用于 3D 楦/底在网页中的统一尺度，与常见市售参考款对应：男 US9、女 US6、童 US13。
 */
export type ShoeAudience = 'MEN' | 'WOMEN' | 'KIDS';

export const SHOE_TARGET_LENGTH_MM: Readonly<Record<ShoeAudience, number>> = {
  MEN: 275,
  WOMEN: 235,
  KIDS: 195,
};

/** 从业务字段识别受众（无则默认 MEN，偏保守） */
export function parseAudience(raw: string | null | undefined): ShoeAudience {
  const s = String(raw ?? '')
    .trim()
    .toUpperCase();
  if (!s) return 'MEN';
  if (/MEN|男|MALE|MR(\s|$)|ADULT(?!S)|GENTS|GENT|男士|男鞋/.test(s)) return 'MEN';
  if (/WOM|女|FEM|LADY|LADIES|WOMEN|WMS|女鞋|女式/.test(s)) return 'WOMEN';
  if (/KID|童|YOUTH|CHILD|JUN|GS|INFANT|幼童|童鞋/.test(s)) return 'KIDS';
  if (/M/.test(s) && s.length <= 3) return 'MEN';
  return 'MEN';
}

export function getTargetLengthMm(aud: ShoeAudience): number {
  return SHOE_TARGET_LENGTH_MM[aud] ?? 275;
}

export function audienceLabelZh(aud: ShoeAudience): string {
  switch (aud) {
    case 'MEN':
      return '男鞋';
    case 'WOMEN':
      return '女鞋';
    case 'KIDS':
      return '童鞋';
    default:
      return '男鞋';
  }
}
