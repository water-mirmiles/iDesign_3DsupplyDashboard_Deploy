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
const STD_MAT = 0xcccccc;

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

function applyForcedMeshMaterial(obj: THREE.Object3D, mat: THREE.MeshStandardMaterial) {
  obj.traverse((ch) => {
    const o = ch as THREE.Mesh;
    if ((o as any).isMesh) {
      const old = o.material;
      if (Array.isArray(old)) old.forEach((m) => (m as THREE.Material).dispose?.());
      else (old as THREE.Material | undefined)?.dispose?.();
      o.material = mat;
      o.castShadow = false;
      o.receiveShadow = false;
    }
  });
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

    const forcedMat = new THREE.MeshStandardMaterial({
      color: STD_MAT,
      side: THREE.DoubleSide,
      metalness: 0.08,
      roughness: 0.5,
    });

    const pointsMaterial = new THREE.PointsMaterial({
      color: STD_MAT,
      size: 2.5,
      sizeAttenuation: true,
    });

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

    const processObj = (obj: THREE.Object3D) => {
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
      // 物性：在显示缩放前（真实坐标）
      const metrics = extractLast3DMetrics(obj);
      onMetricsRef.current?.(metrics);

      // 高面片时仍可用点云，但用灰色点
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
        applyForcedMeshMaterial(obj, forcedMat);
      }

      const { size } = displayNormalizeScale(obj);
      scene.add(obj);

      const m = Math.max(size.x, size.y, size.z, 1e-6);
      camera.position.set(m, m, m);
      camera.near = Math.max(0.0001, m / 5e3);
      camera.far = Math.max(500, m * 1e3);
      camera.lookAt(0, 0, 0);
      camera.updateProjectionMatrix();
      controls.target.set(0, 0, 0);
      controls.update();
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
        processObj(obj);
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
      controls.update();
      renderer.render(scene, camera);
    };
    rafId = window.requestAnimationFrame(tick);

    return () => {
      signal.aborted = true;
      window.cancelAnimationFrame(rafId);
      ro.disconnect();
      disposeGeometries(scene);
      scene.clear();
      forcedMat.dispose();
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
