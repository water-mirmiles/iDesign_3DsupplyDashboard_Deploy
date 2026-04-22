import React, { useEffect, useMemo, useRef, useState } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { OBJLoader } from 'three/examples/jsm/loaders/OBJLoader.js';
import { getStorageBaseUrl } from '@/lib/storageBaseUrl';
import { extractLast3DMetrics, type Last3DMetrics } from '@/lib/last3dMetrics';

type Props = {
  fileUrl: string;
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

const TARGET_MAX = 5;

/**
 * 先居中（物理坐标可极大），物性在缩放前计算；再统一缩放到标准尺度便于入镜
 */
function centerObject(obj: THREE.Object3D) {
  const box = new THREE.Box3().setFromObject(obj);
  const c = box.getCenter(new THREE.Vector3());
  obj.position.sub(c);
  obj.updateMatrixWorld(true);
}

function displayNormalizeScale(obj: THREE.Object3D) {
  obj.updateMatrixWorld(true);
  const box = new THREE.Box3().setFromObject(obj);
  const size = box.getSize(new THREE.Vector3());
  const maxDim = Math.max(size.x, size.y, size.z, 1e-9);
  const s = TARGET_MAX / maxDim;
  obj.scale.multiplyScalar(s);
  obj.updateMatrixWorld(true);
  const b2 = new THREE.Box3().setFromObject(obj);
  const c2 = b2.getCenter(new THREE.Vector3());
  obj.position.sub(c2);
  obj.updateMatrixWorld(true);
  return { scale: s, maxDim, size: b2.getSize(new THREE.Vector3()) };
}

function applyDebugWireframeMaterial(obj: THREE.Object3D, mat: THREE.MeshBasicMaterial) {
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

export default function ThreeDViewer({ fileUrl, className, onLoaded, onError, onMetrics }: Props) {
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
    scene.background = new THREE.Color(0x333333);

    const camera = new THREE.PerspectiveCamera(50, 1, 0.001, 1_000_000);
    camera.position.set(8, 8, 8);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false, powerPreference: 'high-performance' });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.4;
    renderer.shadowMap.enabled = false;
    container.appendChild(renderer.domElement);

    // —— 强力光照：环境 + 半球 + 大平行光 + 点光（强度为原先量级约 3–4 倍）
    const ambient = new THREE.AmbientLight(0xffffff, 2.0);
    scene.add(ambient);
    const hemi = new THREE.HemisphereLight(0xffffff, 0x6b7280, 1.2);
    hemi.position.set(0, 200, 0);
    scene.add(hemi);
    const dir = new THREE.DirectionalLight(0xffffff, 2.4);
    dir.position.set(200, 400, 250);
    scene.add(dir);
    const dir2 = new THREE.DirectionalLight(0xffffff, 1.5);
    dir2.position.set(-300, 100, -200);
    scene.add(dir2);
    const pt = new THREE.PointLight(0xffffff, 3.0, 0, 0.3);
    pt.position.set(0, 80, 0);
    scene.add(pt);
    const pt2 = new THREE.PointLight(0xffeedd, 2.0, 0, 0.2);
    pt2.position.set(200, 50, 200);
    scene.add(pt2);

    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.06;
    controls.screenSpacePanning = true;
    controls.target.set(0, 0, 0);
    controls.minDistance = 0.05;
    controls.maxDistance = 5e3;

    /** 全白线框，不依赖光照，便于判断是否为打光/材质问题 */
    const wireMat = new THREE.MeshBasicMaterial({ color: 0xffffff, wireframe: true, side: THREE.DoubleSide });
    const pointsMaterial = new THREE.PointsMaterial({ color: 0xffffff, size: 2.5, sizeAttenuation: true });

    const debugState = { boxHelper: null as THREE.BoxHelper | null };

    const grid = new THREE.GridHelper(100, 10, 0x666666, 0x4a4a4a);
    scene.add(grid);
    const axes = new THREE.AxesHelper(100);
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
        applyDebugWireframeMaterial(obj, wireMat);
      }

      const { size } = displayNormalizeScale(obj);
      const auditNorm = collectGeometryAudit(obj);
      logGeometryDiagnostics(urlKey, '显示归一化后 (TARGET_MAX=5)', auditNorm, undefined);

      scene.add(obj);
      debugState.boxHelper = new THREE.BoxHelper(obj, 0xffff00);
      scene.add(debugState.boxHelper);

      const m = Math.max(size.x, size.y, size.z, 1e-6);
      camera.position.set(m, m, m);
      camera.near = Math.max(0.0001, m / 5e3);
      camera.far = Math.max(500, m * 1e3);
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
      wireMat.dispose();
      pointsMaterial.dispose();
      controls.dispose();
      try {
        container.removeChild(renderer.domElement);
      } catch {
        // ignore
      }
      renderer.dispose();
    };
  }, [urlKey]);

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
