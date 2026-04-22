/**
 * Node 端与前端 last3dMetrics 对齐的 3D 测量（从 GLB 解析后计算 bbox / 体积 / 码数建议）
 */
import fse from 'fs-extra';
import path from 'node:path';
import * as THREE from 'three';
import { GLTFLoader } from 'three/examples/jsm/loaders/GLTFLoader.js';

function round1(n) {
  return Math.round(n * 10) / 10;
}

function centerObject(obj) {
  const box = new THREE.Box3().setFromObject(obj);
  const c = box.getCenter(new THREE.Vector3());
  obj.position.sub(c);
  obj.updateMatrixWorld(true);
}

function signedVolumeFromBufferGeometry(geom, world) {
  const pos = geom.getAttribute('position');
  if (!pos) return 0;
  const v0 = new THREE.Vector3();
  const v1 = new THREE.Vector3();
  const v2 = new THREE.Vector3();
  const tri = (ia, ib, ic) => {
    v0.fromBufferAttribute(pos, ia).applyMatrix4(world);
    v1.fromBufferAttribute(pos, ib).applyMatrix4(world);
    v2.fromBufferAttribute(pos, ic).applyMatrix4(world);
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

function estimateHeelHeightMm(root) {
  const box = new THREE.Box3().setFromObject(root);
  const { min, max } = box;
  const zSpan = max.z - min.z;
  if (zSpan < 1e-6) return null;
  const zBack0 = min.z;
  const zBack1 = min.z + zSpan * 0.12;
  let yMin = Infinity;
  let yHeel = -Infinity;
  root.updateMatrixWorld(true);
  root.traverse((ch) => {
    const o = ch;
    if (!o.isMesh || !o.geometry) return;
    const g = o.geometry;
    if (!g.getAttribute('position')) return;
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

function hintShoeSizesFromLengthMm(lengthMm) {
  if (!Number.isFinite(lengthMm) || lengthMm <= 0) {
    return { cn: '—', eur: '—' };
  }
  const lenCm = lengthMm / 10;
  const eur = Math.round(1.35 * lenCm + 5);
  const cn = Math.round(lenCm * 2 - 10);
  return {
    cn: `≈ ${Math.max(16, Math.min(52, cn))}（内长/楦长估算）`,
    eur: `≈ ${Math.max(16, Math.min(52, eur))}（内长/楦长估算）`,
  };
}

function extractLast3DMetrics(root) {
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
    if (!ch.isMesh || !ch.geometry) return;
    const g = ch.geometry;
    g.computeVertexNormals();
    const v = signedVolumeFromBufferGeometry(g, ch.matrixWorld);
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

function disposeObject3D(root) {
  root.traverse((ch) => {
    if (ch.geometry) ch.geometry.dispose();
    if (ch.material) {
      if (Array.isArray(ch.material)) ch.material.forEach((m) => m.dispose?.());
      else ch.material.dispose?.();
    }
  });
}

/**
 * 从已保存的 .glb 计算与前端一致的 Last3DMetrics（纯 JSON 可序列化）
 * @param {string} glbPath 绝对路径
 */
export async function computeMetricsFromGlbPath(glbPath) {
  const buf = await fse.readFile(glbPath);
  const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
  const loader = new GLTFLoader();
  const gltf = await new Promise((resolve, reject) => {
    loader.parse(
      ab,
      path.dirname(glbPath) + path.sep,
      (g) => resolve(g),
      (err) => reject(err)
    );
  });
  const root = gltf.scene;
  centerObject(root);
  const metrics = extractLast3DMetrics(root);
  disposeObject3D(root);
  return metrics;
}
