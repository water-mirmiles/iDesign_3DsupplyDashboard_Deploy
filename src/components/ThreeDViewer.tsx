import React, { useEffect, useMemo, useRef, useState } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { OBJLoader } from 'three/examples/jsm/loaders/OBJLoader.js';
import { getStorageBaseUrl } from '@/lib/storageBaseUrl';
import { extractLast3DMetrics, type Last3DMetrics } from '@/lib/last3dMetrics';
import { audienceLabelZh, getTargetLengthMm, parseAudience, type ShoeAudience } from '@/lib/shoeStandards';

type Props = {
  fileUrl: string;
  /**
   * 与清单行 `target_audience` 对齐；未传时按 MEN(275mm) 处理。
   */
  targetAudience?: string;
  className?: string;
  onLoaded?: () => void;
  onError?: (err: Error) => void;
  onMetrics?: (m: Last3DMetrics) => void;
};

function resolveAssetUrl(fileUrl: string) {
  const u = String(fileUrl || '').trim();
  if (!u) return '';
  if (u.startsWith('http://') || u.startsWith('https://')) return u;
  if (u.startsWith('/')) return `${getStorageBaseUrl()}${u}`;
  return `${getStorageBaseUrl()}/${u.replace(/^\//, '')}`;
}

/**
 * 世界单位采用 mm，便于与行业 Target 对齐
 */
function centerObject(obj: THREE.Object3D) {
  const box = new THREE.Box3().setFromObject(obj);
  const c = box.getCenter(new THREE.Vector3());
  obj.position.sub(c);
  obj.updateMatrixWorld(true);
}

function maxAxisLength(obj: THREE.Object3D) {
  obj.updateMatrixWorld(true);
  const s = new THREE.Box3().setFromObject(obj).getSize(new THREE.Vector3());
  return Math.max(s.x, s.y, s.z, 0);
}

/**
 * 米制(≈0.27) → ×1000；异常大数(2700) → ÷10；再强制缩放到行业目标长度 (mm)
 */
function applyIndustryMmNormalization(
  obj: THREE.Object3D,
  audience: ShoeAudience
): { rawMax: number; lenBeforeTargetMm: number; scaleToTarget: number; targetMm: number; audience: ShoeAudience } {
  const targetMm = getTargetLengthMm(audience);
  const raw0 = maxAxisLength(obj);
  if (raw0 < 0.5 && raw0 > 0.05) {
    obj.scale.multiplyScalar(1000);
  }
  obj.updateMatrixWorld(true);
  let m = maxAxisLength(obj);
  if (m > 1500 && m < 5000) {
    obj.scale.multiplyScalar(0.1);
  }
  obj.updateMatrixWorld(true);
  const lenBeforeTargetMm = maxAxisLength(obj);
  const scaleToTarget = lenBeforeTargetMm < 1e-9 ? 1 : targetMm / lenBeforeTargetMm;
  obj.scale.multiplyScalar(scaleToTarget);
  obj.updateMatrixWorld(true);
  centerObject(obj);
  return { rawMax: raw0, lenBeforeTargetMm, scaleToTarget, targetMm, audience };
}

function applyPhongShading(obj: THREE.Object3D, mat: THREE.MeshPhongMaterial) {
  obj.traverse((ch) => {
    const o = ch as THREE.Mesh;
    if ((o as any).isMesh) {
      const old = o.material;
      if (Array.isArray(old)) old.forEach((m) => (m as THREE.Material).dispose?.());
      else (old as THREE.Material | undefined)?.dispose?.();
      o.material = mat;
    }
  });
}

type GeometryAudit = {
  meshCount: number;
  pointsCount: number;
  vertexCount: number;
  faceCount: number;
  min: THREE.Vector3;
  max: THREE.Vector3;
  center: THREE.Vector3;
  size: THREE.Vector3;
};

function collectGeometryAudit(obj: THREE.Object3D): GeometryAudit {
  let meshCount = 0;
  let pointsCount = 0;
  let vertexCount = 0;
  let faceCount = 0;
  obj.updateMatrixWorld(true);
  obj.traverse((ch) => {
    if ((ch as any).isMesh) {
      meshCount += 1;
      const g = (ch as THREE.Mesh).geometry as THREE.BufferGeometry;
      const pos = g.getAttribute('position');
      if (pos) {
        const vc = pos.count;
        vertexCount += vc;
        if (g.index) faceCount += g.index.count / 3;
        else faceCount += vc / 3;
      }
    } else if ((ch as any).isPoints) {
      pointsCount += 1;
      const g = (ch as THREE.Points).geometry as THREE.BufferGeometry;
      const pos = g.getAttribute('position');
      if (pos) vertexCount += pos.count;
    }
  });
  const box = new THREE.Box3().setFromObject(obj);
  const center = box.getCenter(new THREE.Vector3());
  const size = box.getSize(new THREE.Vector3());
  return {
    meshCount,
    pointsCount,
    vertexCount: Math.floor(vertexCount),
    faceCount: Math.floor(faceCount),
    min: box.min.clone(),
    max: box.max.clone(),
    center,
    size,
  };
}

function isGeometryInvalid(a: GeometryAudit) {
  const s = a.size;
  const vol = s.x * s.y * s.z;
  if (a.vertexCount <= 0 && a.faceCount <= 0) return true;
  if (vol < 1e-18) return true;
  if (Math.max(s.x, s.y, s.z) < 1e-12) return true;
  return false;
}

function logPerMeshTable(obj: THREE.Object3D) {
  const rows: { name: string; vertices: number; faces: string }[] = [];
  obj.traverse((ch) => {
    if ((ch as any).isMesh) {
      const g = (ch as THREE.Mesh).geometry as THREE.BufferGeometry;
      const pos = g.getAttribute('position');
      const v = pos?.count ?? 0;
      const f = g.index ? g.index.count / 3 : pos ? pos.count / 3 : 0;
      rows.push({ name: ch.name || '(unnamed)', vertices: v, faces: String(Math.floor(f)) });
    }
  });
  if (rows.length) {
    // eslint-disable-next-line no-console
    console.log('%c[ThreeDViewer] Per-Mesh 明细', 'color:#22c55e;font-weight:bold');
    // eslint-disable-next-line no-console
    console.table(rows);
  }
}

function logGeometryDiagnostics(
  fileUrlResolved: string,
  label: string,
  audit: GeometryAudit,
  obj?: THREE.Object3D
) {
  const gRows = [
    {
      阶段: label,
      文件: fileUrlResolved,
      网格数: audit.meshCount,
      点云子对象数: audit.pointsCount,
      顶点总数: audit.vertexCount,
      面片总数: audit.faceCount,
      'box.min': `${audit.min.x.toFixed(6)}, ${audit.min.y.toFixed(6)}, ${audit.min.z.toFixed(6)}`,
      'box.max': `${audit.max.x.toFixed(6)}, ${audit.max.y.toFixed(6)}, ${audit.max.z.toFixed(6)}`,
      'size(长宽高)': `${audit.size.x.toFixed(6)}, ${audit.size.y.toFixed(6)}, ${audit.size.z.toFixed(6)}`,
      中心: `${audit.center.x.toFixed(6)}, ${audit.center.y.toFixed(6)}, ${audit.center.z.toFixed(6)}`,
    },
  ];
  // eslint-disable-next-line no-console
  console.log(`%c[ThreeDViewer] Geometry — ${label}`, 'color:#0ea5e9;font-weight:bold');
  // eslint-disable-next-line no-console
  console.table(gRows);
  if (obj) logPerMeshTable(obj);
}

function logCameraDiagnostics(camera: THREE.PerspectiveCamera) {
  // eslint-disable-next-line no-console
  console.log('%c[ThreeDViewer] Camera (当前帧)', 'color:#a855f7;font-weight:bold');
  // eslint-disable-next-line no-console
  console.table([
    {
      'camera.position': `${camera.position.x.toFixed(4)}, ${camera.position.y.toFixed(4)}, ${camera.position.z.toFixed(4)}`,
      'camera.near': camera.near,
      'camera.far': camera.far,
      fov: camera.fov,
      aspect: camera.aspect,
    },
  ]);
}

function loadTextWithXhr(
  url: string,
  onProgress: (pct: number | null) => void,
  signal: { aborted: boolean }
): Promise<string> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('GET', url, true);
    xhr.responseType = 'text';
    xhr.onprogress = (e) => {
      if (signal.aborted) return;
      if (e.lengthComputable && e.total > 0) {
        onProgress(Math.max(0, Math.min(100, Math.round((e.loaded / e.total) * 100))));
      } else {
        onProgress(null);
      }
    };
    xhr.onload = () => {
      if (signal.aborted) return;
      onProgress(100);
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(xhr.responseText as string);
        return;
      }
      // eslint-disable-next-line no-console
      console.error('[ThreeDViewer] OBJ HTTP error', xhr.status, xhr.statusText, url);
      reject(new Error(`HTTP ${xhr.status} ${String(xhr.statusText || '').trim()}`));
    };
    xhr.onerror = () => {
      if (signal.aborted) return;
      // eslint-disable-next-line no-console
      console.error('[ThreeDViewer] XHR network error', url);
      reject(new Error('网络错误或无法访问资源（CORS/代理）'));
    };
    xhr.send();
  });
}

export default function ThreeDViewer({ fileUrl, targetAudience, className, onLoaded, onError, onMetrics }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const onMetricsRef = useRef(onMetrics);
  const onLoadedRef = useRef(onLoaded);
  const onErrorRef = useRef(onError);
  onMetricsRef.current = onMetrics;
  onLoadedRef.current = onLoaded;
  onErrorRef.current = onError;

  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<number | null>(null);

  const urlKey = useMemo(() => resolveAssetUrl(String(fileUrl || '').trim()), [fileUrl]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    if (!urlKey) return;

    const signal = { aborted: false };

    setReady(false);
    setError(null);
    setProgress(0);

    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0xeeeeee);

    const camera = new THREE.PerspectiveCamera(50, 1, 0.001, 1_000_000);
    camera.position.set(8, 8, 8);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false, powerPreference: 'high-performance' });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.4;
    renderer.shadowMap.enabled = false;
    container.appendChild(renderer.domElement);

    const ambient = new THREE.AmbientLight(0xffffff, 0.75);
    scene.add(ambient);
    const hemi = new THREE.HemisphereLight(0xffffff, 0xe5e5e5, 0.45);
    hemi.position.set(0, 1, 0);
    scene.add(hemi);
    const dir = new THREE.DirectionalLight(0xffffff, 0.9);
    dir.position.set(400, 800, 500);
    scene.add(dir);
    const fill = new THREE.DirectionalLight(0xffffff, 0.4);
    fill.position.set(-300, 200, -200);
    scene.add(fill);

    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.06;
    controls.screenSpacePanning = true;
    controls.target.set(0, 0, 0);
    controls.minDistance = 0.05;
    controls.maxDistance = 5e3;

    const phongMat = new THREE.MeshPhongMaterial({
      color: 0x888888,
      flatShading: true,
      side: THREE.DoubleSide,
      shininess: 24,
    });
    const pointsMaterial = new THREE.PointsMaterial({ color: 0x666666, size: 2.5, sizeAttenuation: true });

    const debugState = { boxHelper: null as THREE.BoxHelper | null };

    // 1 world unit = 1mm，格线每格 10mm（200/20=10）
    const grid = new THREE.GridHelper(200, 20, 0xbbbbbb, 0xcccccc);
    grid.position.y = 0;
    scene.add(grid);
    const axes = new THREE.AxesHelper(150);
    scene.add(axes);

    const resize = () => {
      const w = container.clientWidth || 1;
      const h = container.clientHeight || 1;
      camera.aspect = w / h;
      camera.updateProjectionMatrix();
      renderer.setSize(w, h, false);
    };

    const ro = new ResizeObserver(() => resize());
    ro.observe(container);
    resize();

    const loader = new OBJLoader();
    const disposeGeometries = (o: THREE.Object3D) => {
      o.traverse((n) => {
        const m = n as any;
        if (m.geometry && m.geometry.dispose) m.geometry.dispose();
        // 材质为共享 forcedMat / pointsMaterial，不逐节点 dispose
      });
    };

    const processObj = (obj: THREE.Object3D): boolean => {
      let tri = 0;
      const meshes: THREE.Mesh[] = [];
      obj.traverse((ch) => {
        const m = ch as THREE.Mesh;
        if ((m as any).isMesh) meshes.push(m);
      });
      for (const mesh of meshes) {
        const geom = (mesh as any).geometry as THREE.BufferGeometry | undefined;
        if (geom?.isBufferGeometry) {
          try {
            geom.computeVertexNormals();
          } catch {
            // ignore
          }
          if (geom.index) tri += Math.floor(geom.index.count / 3);
          else {
            const p = geom.getAttribute('position');
            if (p) tri += Math.floor(p.count / 3);
          }
        }
      }

      centerObject(obj);
      const auditCentered = collectGeometryAudit(obj);
      logGeometryDiagnostics(urlKey, '已居中，未做显示缩放', auditCentered, obj);

      if (isGeometryInvalid(auditCentered)) {
        const msg = '错误：模型几何体数据为空或尺寸异常';
        setError(msg);
        onErrorRef.current?.(new Error(msg));
        // eslint-disable-next-line no-console
        console.error('[ThreeDViewer] Geometry invalid', auditCentered);
        return false;
      }

      const aud = parseAudience(targetAudience);
      const sync = applyIndustryMmNormalization(obj, aud);
      // eslint-disable-next-line no-console
      console.log(
        `[StandardSync] 类别: ${audienceLabelZh(sync.audience)} | 原始长度: ${sync.rawMax} | 缩放系数: ${sync.scaleToTarget.toFixed(4)} | 最终长度: ${sync.targetMm}mm`
      );

      const auditNorm = collectGeometryAudit(obj);
      logGeometryDiagnostics(urlKey, '行业归一化后 (mm)', auditNorm, undefined);

      const metrics = extractLast3DMetrics(obj);
      onMetricsRef.current?.(metrics);

      const huge = tri >= 1_500_000;
      if (huge) {
        obj.traverse((ch) => {
          const m = ch as THREE.Mesh;
          if (!(m as any).isMesh) return;
          const geom = m.geometry;
          if (!geom) return;
          const pts = new THREE.Points(geom, pointsMaterial);
          if (m.parent) {
            m.parent.add(pts);
            m.parent.remove(m);
          }
        });
      } else {
        applyPhongShading(obj, phongMat);
      }

      scene.add(obj);
      debugState.boxHelper = new THREE.BoxHelper(obj, 0xff8800);
      scene.add(debugState.boxHelper);

      const D = sync.targetMm * 1.5;
      const off = new THREE.Vector3(0.7, 0.45, 0.8).normalize().multiplyScalar(D);
      camera.position.copy(off);
      camera.near = Math.max(0.1, D / 2000);
      camera.far = D * 80;
      camera.lookAt(0, 0, 0);
      camera.updateProjectionMatrix();
      controls.target.set(0, 0, 0);
      controls.update();
      logCameraDiagnostics(camera);
      return true;
    };

    void (async () => {
      try {
        const text = await loadTextWithXhr(
          urlKey,
          (p) => {
            if (!signal.aborted) setProgress(p);
          },
          signal
        );
        if (signal.aborted) return;
        const obj = loader.parse(text);
        // eslint-disable-next-line no-console
        console.log('%c[ThreeDViewer] OBJ 文本已解析，fileUrl:', 'color:#f59e0b', urlKey, ' 长度:', text.length);
        const ok = processObj(obj);
        if (!ok) {
          setProgress(null);
          return;
        }
        setReady(true);
        onLoadedRef.current?.();
      } catch (e) {
        if (signal.aborted) return;
        const err = e instanceof Error ? e : new Error('加载失败');
        // eslint-disable-next-line no-console
        console.error('[ThreeDViewer] load/parse', err);
        setError(err.message);
        onErrorRef.current?.(err);
        setProgress(null);
      }
    })();

    let rafId = 0;
    const tick = () => {
      rafId = window.requestAnimationFrame(tick);
      if (signal.aborted) return;
      if (debugState.boxHelper) debugState.boxHelper.update();
      controls.update();
      renderer.render(scene, camera);
    };
    rafId = window.requestAnimationFrame(tick);

    return () => {
      signal.aborted = true;
      window.cancelAnimationFrame(rafId);
      ro.disconnect();
      if (debugState.boxHelper) {
        try {
          scene.remove(debugState.boxHelper);
          debugState.boxHelper.traverse((n) => {
            const m = n as any;
            if (m.geometry) m.geometry.dispose();
            if (m.material) {
              if (Array.isArray(m.material)) m.material.forEach((x: THREE.Material) => x.dispose());
              else m.material.dispose();
            }
          });
        } catch {
          // ignore
        }
        debugState.boxHelper = null;
      }
      disposeGeometries(scene);
      scene.clear();
      phongMat.dispose();
      pointsMaterial.dispose();
      controls.dispose();
      try {
        container.removeChild(renderer.domElement);
      } catch {
        // ignore
      }
      renderer.dispose();
    };
  }, [urlKey, targetAudience]);

  return (
    <div className={className} ref={containerRef} style={{ position: 'relative', minHeight: 120 }}>
      {!ready && !error ? (
        <div className="absolute inset-0 pointer-events-none z-[1]">
          {progress != null && progress < 100 ? (
            <div className="absolute left-4 right-4 bottom-4 text-xs text-slate-200/90">
              <div className="flex items-center justify-between mb-1">
                <span>OBJ 拉取中</span>
                <span>{typeof progress === 'number' ? `${progress}%` : '—'}</span>
              </div>
              {typeof progress === 'number' ? (
                <div className="h-1.5 bg-white/10 rounded">
                  <div className="h-1.5 bg-blue-500 rounded" style={{ width: `${progress}%` }} />
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}
      {error ? (
        <div
          className="absolute inset-0 z-20 flex flex-col items-center justify-center px-4 text-center bg-black/75"
          data-three-error-overlay="1"
        >
          <p className="text-sm font-mono text-red-400 break-all max-w-full">3D 加载失败</p>
          <p className="text-lg font-mono text-red-300 font-bold mt-2 break-all max-w-full">{error}</p>
          <p className="text-xs text-slate-400 mt-2">请检查 URL、HTTP 状态与跨域/代理 (Vite → :3001)</p>
        </div>
      ) : null}
    </div>
  );
}
