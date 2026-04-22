import React, { useEffect, useMemo, useRef, useState } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { OBJLoader } from 'three/examples/jsm/loaders/OBJLoader.js';
import { getStorageBaseUrl } from '@/lib/storageBaseUrl';
import { extractLast3DMetrics, type Last3DMetrics } from '@/lib/last3dMetrics';

type Props = {
  /** 可传相对路径 /storage/... 或绝对 http(s) 完整 URL */
  fileUrl: string;
  className?: string;
  onLoaded?: () => void;
  onError?: (err: Error) => void;
  /** 3D 解析成功后的物性参数 */
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
 * 将模型包围球完整纳入视野，按纵横比同时考虑水平/竖直 FOV
 */
function fitCameraToObject(camera: THREE.PerspectiveCamera, object: THREE.Object3D, controls?: OrbitControls) {
  object.updateMatrixWorld(true);
  const box = new THREE.Box3().setFromObject(object);
  const center = box.getCenter(new THREE.Vector3());
  const sphere = new THREE.Sphere();
  box.getBoundingSphere(sphere);
  const r = Math.max(1e-6, sphere.radius);
  const vFov = (camera.fov * Math.PI) / 180;
  const aspect = Math.max(0.2, Math.min(5, camera.aspect));
  const hFov = 2 * Math.atan(Math.tan(vFov / 2) * aspect);
  const distV = r / Math.sin(vFov / 2);
  const distH = r / Math.sin(hFov / 2);
  const d = Math.max(distV, distH) * 1.12;

  camera.position.set(center.x, center.y + r * 0.12, center.z + d);
  const near = Math.max(0.0001, d / 1e4);
  const far = d * 400;
  camera.near = near;
  camera.far = far;
  camera.updateProjectionMatrix();

  if (controls) {
    controls.target.copy(center);
    controls.minDistance = Math.max(0.0001, d * 0.0005);
    controls.maxDistance = d * 200;
    controls.update();
  }
}

export default function ThreeDViewer({ fileUrl, className, onLoaded, onError, onMetrics }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const rendererRef = useRef<THREE.WebGLRenderer | null>(null);
  const rafRef = useRef<number | null>(null);
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

    setReady(false);
    setError(null);
    setProgress(null);

    const manager = new THREE.LoadingManager();
    manager.onError = (u) => {
      // eslint-disable-next-line no-console
      console.error('[ThreeDViewer] LoadingManager.onError', u);
    };

    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x0b1220);

    const camera = new THREE.PerspectiveCamera(45, 1, 0.0001, 1_000_000);
    camera.position.set(0, 0.5, 3);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false, powerPreference: 'high-performance' });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.0;
    renderer.shadowMap.enabled = false;

    rendererRef.current = renderer;
    container.appendChild(renderer.domElement);

    const ambient = new THREE.AmbientLight(0xffffff, 0.55);
    scene.add(ambient);
    const hemi = new THREE.HemisphereLight(0xffffff, 0x1f2937, 0.75);
    hemi.position.set(0, 1, 0);
    scene.add(hemi);
    const dir = new THREE.DirectionalLight(0xffffff, 0.7);
    dir.position.set(2, 3, 2);
    scene.add(dir);

    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;
    controls.screenSpacePanning = true;

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

    let disposed = false;
    const loader = new OBJLoader(manager);

    const material = new THREE.MeshStandardMaterial({
      color: new THREE.Color('#e5e7eb'),
      roughness: 0.55,
      metalness: 0.05,
    });

    const pointsMaterial = new THREE.PointsMaterial({
      color: new THREE.Color('#e5e7eb'),
      size: 1.5,
      sizeAttenuation: true,
    });

    const applyMaterialAndOptimize = (obj: THREE.Object3D) => {
      let totalTriangles = 0;
      const meshes: THREE.Mesh[] = [];
      obj.traverse((child) => {
        const mesh = child as THREE.Mesh;
        if ((mesh as any).isMesh) meshes.push(mesh);
      });

      for (const mesh of meshes) {
        const geom = (mesh as any).geometry as THREE.BufferGeometry | undefined;
        if (geom && geom.isBufferGeometry) {
          try {
            geom.computeVertexNormals();
          } catch {
            // 非法面片时可能抛错
          }
          try {
            geom.computeBoundingBox();
          } catch {
            // ignore
          }
          const pos = geom.getAttribute('position');
          if (geom.index) totalTriangles += Math.floor((geom.index.count || 0) / 3);
          else if (pos) totalTriangles += Math.floor((pos.count || 0) / 3);
        }
      }

      // 仅对根对象整体居中，避免多 mesh 各自 geometry.center() 破坏装配关系
      try {
        const b = new THREE.Box3().setFromObject(obj);
        const c = b.getCenter(new THREE.Vector3());
        obj.position.sub(c);
        obj.updateMatrixWorld(true);
      } catch {
        // ignore
      }

      const TRIANGLE_POINTS_FALLBACK = 1_500_000;
      const shouldUsePoints = totalTriangles >= TRIANGLE_POINTS_FALLBACK;

      obj.traverse((child) => {
        const mesh = child as THREE.Mesh;
        if (!(mesh as any).isMesh) return;
        const geom = (mesh as any).geometry as THREE.BufferGeometry | undefined;
        if (!geom || !geom.isBufferGeometry) return;

        if (shouldUsePoints) {
          const pts = new THREE.Points(geom, pointsMaterial);
          pts.position.copy(mesh.position);
          pts.rotation.copy(mesh.rotation);
          pts.scale.copy(mesh.scale);
          if (mesh.parent) {
            mesh.parent.add(pts);
            mesh.parent.remove(mesh);
          }
          return;
        }
        mesh.material = material;
        mesh.castShadow = false;
        mesh.receiveShadow = false;
      });

      return { usedPoints: shouldUsePoints };
    };

    try {
      loader.load(
        urlKey,
        (obj) => {
          if (disposed) return;
          try {
            applyMaterialAndOptimize(obj);
            scene.add(obj);
            fitCameraToObject(camera, obj, controls);

            const metrics = extractLast3DMetrics(obj);
            onMetricsRef.current?.(metrics);
            setReady(true);
            onLoadedRef.current?.();
          } catch (e) {
            // eslint-disable-next-line no-console
            console.error('[ThreeDViewer] post-load / metrics failed', e);
            const err = e instanceof Error ? e : new Error('模型后处理失败');
            setError(err.message);
            onErrorRef.current?.(err);
          }
        },
        (evt) => {
          if (disposed) return;
          const loaded = Number((evt as any)?.loaded || 0);
          const total = Number((evt as any)?.total || 0);
          if (total > 0) {
            const p = Math.max(0, Math.min(100, Math.round((loaded / total) * 100)));
            setProgress(p);
          } else {
            setProgress(null);
          }
        },
        (e) => {
          if (disposed) return;
          // eslint-disable-next-line no-console
          console.error('[ThreeDViewer] OBJLoader error', e);
          const err = e instanceof Error ? e : new Error((e as any)?.message || 'OBJ 加载失败');
          setError(err.message);
          onErrorRef.current?.(err);
        }
      );
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error('[ThreeDViewer] loader.load throw', e);
      const err = e instanceof Error ? e : new Error('OBJ 加载异常');
      setError(err.message);
      onErrorRef.current?.(err);
    }

    const tick = () => {
      controls.update();
      renderer.render(scene, camera);
      rafRef.current = window.requestAnimationFrame(tick);
    };
    tick();

    return () => {
      disposed = true;
      ro.disconnect();
      if (rafRef.current != null) window.cancelAnimationFrame(rafRef.current);
      controls.dispose();

      scene.traverse((o) => {
        if ((o as any).isMesh || (o as any).isPoints) {
          const geom = (o as any).geometry as THREE.BufferGeometry | undefined;
          if (geom && typeof geom.dispose === 'function') geom.dispose();
        }
      });
      material.dispose();
      pointsMaterial.dispose();

      try {
        container.removeChild(renderer.domElement);
      } catch {
        // ignore
      }
      renderer.dispose();
      rendererRef.current = null;
    };
  }, [urlKey]);

  return (
    <div className={className} ref={containerRef}>
      {!ready && !error ? (
        <div className="absolute inset-0 pointer-events-none">
          {progress != null ? (
            <div className="absolute left-4 right-4 bottom-4 text-xs text-slate-200/80">
              <div className="flex items-center justify-between mb-1">
                <span>OBJ 下载中</span>
                <span>{progress}%</span>
              </div>
              <div className="h-1.5 bg-white/10 rounded">
                <div className="h-1.5 bg-blue-500 rounded" style={{ width: `${progress}%` }} />
              </div>
            </div>
          ) : null}
        </div>
      ) : null}
      {error ? (
        <div className="absolute inset-0 flex items-center justify-center text-sm text-red-300 bg-black/20">
          {error}
        </div>
      ) : null}
    </div>
  );
}
