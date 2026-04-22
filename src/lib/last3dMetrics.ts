import * as THREE from 'three';

export type Last3DMetrics = {
  /** 长度 (L) — X 轴跨度 */
  lengthMm: number;
  /** 宽度 (W) — Z 轴跨度 */
  widthMm: number;
  /** 高度 (H) — Y 轴跨度 */
  heightMm: number;
  unit: 'mm';
  bboxMin: [number, number, number];
  bboxMax: [number, number, number];
  /** 估算跟高：后掌区域(沿 Z)相对“地面”的抬升，工业坐标系为近似 */
  heelHeightEstimateMm: number | null;
  /** 中国码建议（由楦长 mm 粗算，仅供对照） */
  shoeSizeChinaHint: string;
  /** 欧码建议（由楦长 mm 粗算，仅供对照） */
  shoeSizeEurHint: string;
  /** 闭合三角网格体积 (mm³)，不闭合时仍返回绝对值作参考并标记 */
  volumeMm3: number | null;
  /** 体积算法是否自洽为闭合流形（无严格验证，仅作提示） */
  volumeLikelyClosed: boolean;
  volumeNote: string;
};

function round1(n: number) {
  return Math.round(n * 10) / 10;
}

function fmtMm(n: number) {
  if (!Number.isFinite(n)) return '—';
  return `${round1(n)} mm`;
}

/**
 * 对单个 BufferGeometry 计算有符号体积累加（已乘 mesh.matrixWorld 变换到世界坐标）
 */
function signedVolumeFromBufferGeometry(geom: THREE.BufferGeometry, world: THREE.Matrix4) {
  const pos = geom.getAttribute('position');
  if (!pos) return 0;
  const v0 = new THREE.Vector3();
  const v1 = new THREE.Vector3();
  const v2 = new THREE.Vector3();
  const tri = (ia: number, ib: number, ic: number) => {
    v0.fromBufferAttribute(pos, ia).applyMatrix4(world);
    v1.fromBufferAttribute(pos, ib).applyMatrix4(world);
    v2.fromBufferAttribute(pos, ic).applyMatrix4(world);
    // 四面体 (O, v0, v1, v2) 有符号体积累加
    return v0.dot(v1.clone().cross(v2)) / 6;
  };
  let s = 0;
  if (geom.index) {
    const idx = geom.index;
    for (let i = 0; i + 2 < idx.count; i += 3) {
      s += tri(idx.getX(i), idx.getX(i + 1), idx.getX(i + 2));
    }
  } else {
    for (let i = 0; i + 2 < pos.count; i += 3) {
      s += tri(i, i + 1, i + 2);
    }
  }
  return s;
}

/**
 * 粗算跟高：取 Z 方向后缘窄带(鞋跟侧) 点的最高 Y 与全局最低 Y 之差（假设 last 大致沿 +Z/−Z 为前后）
 */
function estimateHeelHeightMm(root: THREE.Object3D) {
  const box = new THREE.Box3().setFromObject(root);
  const { min, max } = box;
  const zSpan = max.z - min.z;
  if (zSpan < 1e-6) return null;
  const zBack0 = min.z;
  const zBack1 = min.z + zSpan * 0.12;
  let yMin = Infinity;
  let yHeel = -Infinity; // 后掌带内最高点
  root.updateMatrixWorld(true);
  root.traverse((ch) => {
    const o = ch as THREE.Mesh;
    if (!o.isMesh || !o.geometry) return;
    const g = o.geometry;
    if (!g.getAttribute('position')) return;
    g.computeBoundingBox();
    const pos = g.getAttribute('position');
    const mat = o.matrixWorld;
    for (let i = 0; i < pos.count; i++) {
      const p = new THREE.Vector3().fromBufferAttribute(pos, i).applyMatrix4(mat);
      if (p.y < yMin) yMin = p.y;
      if (p.z >= zBack0 && p.z <= zBack1 && p.y > yHeel) yHeel = p.y;
    }
  });
  if (!Number.isFinite(yMin) || !Number.isFinite(yHeel)) return null;
  const h = yHeel - yMin;
  return h > 0 && h < zSpan * 2 ? h : null;
}

function hintShoeSizesFromLengthMm(lengthMm: number) {
  if (!Number.isFinite(lengthMm) || lengthMm <= 0) {
    return { cn: '—', eur: '—' as string };
  }
  const lenCm = lengthMm / 10;
  // 常见粗算：欧码约 = 1.4 * 脚长(cm) + 常数(经验)；中国码 = 2*脚长(cm)-10（鞋内长近似）
  const eur = Math.round(1.35 * lenCm + 5);
  const cn = Math.round(lenCm * 2 - 10);
  return {
    cn: `≈ ${Math.max(16, Math.min(52, cn))}（内长/楦长估算）`,
    eur: `≈ ${Math.max(16, Math.min(52, eur))}（内长/楦长估算）`,
  };
}

/**
 * 从已居中/变换后的对象提取报告（L=X, W=Z, H=Y；单位 mm 与源坐标一致，假设工业导出为 mm）
 */
export function extractLast3DMetrics(root: THREE.Object3D): Last3DMetrics {
  root.updateMatrixWorld(true);
  const box = new THREE.Box3().setFromObject(root);
  const size = box.getSize(new THREE.Vector3());
  const { min, max } = box;

  const lengthMm = size.x;
  const widthMm = size.z;
  const heightMm = size.y;

  const { cn, eur } = hintShoeSizesFromLengthMm(lengthMm);
  const heelH = estimateHeelHeightMm(root);

  let volSum = 0;
  let meshParts = 0;
  root.traverse((ch) => {
    const o = ch as THREE.Mesh;
    if (!o.isMesh || !o.geometry) return;
    const g = o.geometry;
    g.computeVertexNormals();
    const v = signedVolumeFromBufferGeometry(g, o.matrixWorld);
    if (Number.isFinite(v)) {
      volSum += v;
      meshParts += 1;
    }
  });

  const absVol = Math.abs(volSum);
  const likelyClosed = meshParts > 0 && absVol > 1e-3;
  const volumeNote = likelyClosed
    ? '基于三角面片有符号体积累加；若网格外壳非水密，该值为近似。'
    : '未能可靠估算闭合体积：模型可能不闭合/仅点云/法线不兼容。';

  return {
    lengthMm: round1(lengthMm),
    widthMm: round1(widthMm),
    heightMm: round1(heightMm),
    unit: 'mm',
    bboxMin: [round1(min.x), round1(min.y), round1(min.z)],
    bboxMax: [round1(max.x), round1(max.y), round1(max.z)],
    heelHeightEstimateMm: heelH != null ? round1(heelH) : null,
    shoeSizeChinaHint: cn,
    shoeSizeEurHint: eur,
    volumeMm3: likelyClosed ? round1(absVol) : null,
    volumeLikelyClosed: likelyClosed,
    volumeNote,
  };
}

export function formatMetricsForUi(m: Last3DMetrics) {
  return {
    L: fmtMm(m.lengthMm),
    W: fmtMm(m.widthMm),
    H: fmtMm(m.heightMm),
    heel: m.heelHeightEstimateMm != null ? fmtMm(m.heelHeightEstimateMm) : '—',
    vol: m.volumeMm3 != null ? `${(m.volumeMm3 / 1_000_000).toFixed(3)} cm³` : '—',
  };
}
